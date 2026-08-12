// SPDX-License-Identifier: MIT or GPL-2.0-only

#include <config.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <sched.h>
#include <pthread.h>
#include <getopt.h>
#include <stdarg.h>
#include <errno.h>
#include <error.h>
#include <limits.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

#include "ublksrv.h"
#include "ublksrv_utils.h"

/*
 * Buffer pool tiers for UBLK_F_BUF_RINGS / UBLK_F_PINNED_BUFS.
 * Small (4K) buffers cover metadata, larger tiers cover bigger IOs.
 * The largest tier must be >= max_io_buf_bytes.
 */
static struct ublksrv_buf_pool_spec demo_pool_tiers[UBLK_MAX_BUF_POOLS] = {
	{ 4096,		8 },
	{ 65536,	4 },
	{ 512 * 1024,	2 },	/* >= DEF_BUF_SIZE */
};
static unsigned int demo_nr_pool_tiers = 3;
static bool demo_pool_tiers_explicit;

struct demo_queue_startup {
	pthread_mutex_t lock;
	pthread_cond_t cond;
	unsigned int ready;
	bool failed;
	bool release;
};

struct demo_queue_info {
	const struct ublksrv_dev *dev;
	int qid;
	pthread_t thread;
	struct ublksrv_buf_pool_set *pool_set;
	struct demo_queue_startup *startup;
};

static bool use_pbuf;
static bool use_pinned_pbuf = true;

static struct ublksrv_ctrl_dev *this_dev;

/*
 * queue_init() only prepares the initial FETCH commands.  Submit all of them
 * before publishing queue readiness: the main thread calls START_DEV only
 * after the startup barrier, and START_DEV itself waits for every FETCH to
 * reach the kernel.  Buffer-pool registration also submits the ring, but the
 * ordinary per-tag-buffer path has no setup command to do that implicitly.
 */
static int demo_park_fetches(const struct ublksrv_queue *q)
{
	while (io_uring_sq_ready(q->ring_ptr)) {
		int ret = io_uring_submit(q->ring_ptr);

		if (ret == -EINTR)
			continue;
		if (ret < 0)
			return ret;
		if (!ret)
			return -EIO;
	}

	return 0;
}

static void sig_handler(int sig)
{
	fprintf(stderr, "got signal %d\n", sig);
	ublksrv_ctrl_stop_dev(this_dev);
}

/*
 * Allocate one anonymous arena and atomically register all configured tiers.
 */
static int demo_setup_buf_pools(const struct ublksrv_queue *q,
				struct demo_queue_info *info)
{
	unsigned int i;
	int ret;

	ret = ublksrv_queue_setup_buf_pools(q, demo_pool_tiers,
					     demo_nr_pool_tiers,
					     &info->pool_set);
	if (ret < 0) {
		fprintf(stderr, "queue %d: register buffer pools failed: %d\n",
			info->qid, ret);
		return ret;
	}

	for (i = 0; i < demo_nr_pool_tiers; i++)
		fprintf(stdout,
			"queue %d: registered pool tier %u: bs=%u n=%u\n",
			info->qid, i, demo_pool_tiers[i].buf_size,
			demo_pool_tiers[i].nr_bufs);

	return 0;
}

static void demo_teardown_buf_pools(struct demo_queue_info *info)
{
	ublksrv_buf_pool_set_destroy(info->pool_set);
	info->pool_set = NULL;
}

/*
 * Keep every queue parked until all queues have completed setup and the main
 * thread has attempted START_DEV.  Buffer-pool registration is setup-only,
 * so START_DEV must not race it.
 */
static bool demo_queue_startup_wait(struct demo_queue_info *info, bool failed)
{
	struct demo_queue_startup *startup = info->startup;
	bool ready;

	pthread_mutex_lock(&startup->lock);
	startup->failed |= failed;
	startup->ready++;
	pthread_cond_broadcast(&startup->cond);
	while (!startup->release)
		pthread_cond_wait(&startup->cond, &startup->lock);
	ready = !startup->failed;
	pthread_mutex_unlock(&startup->lock);

	return ready;
}

/*
 * io handler for each ublkdev's queue
 *
 * Just for showing how to build ublksrv target's io handling, so callers
 * can apply these APIs in their own thread context for making one ublk
 * block device.
 */
static void *demo_null_io_handler_fn(void *data)
{
	struct demo_queue_info *info = (struct demo_queue_info *)data;
	const struct ublksrv_dev *dev = info->dev;
	const struct ublksrv_ctrl_dev *cdev = ublksrv_get_ctrl_dev(dev);
	const struct ublksrv_ctrl_dev_info *dinfo = ublksrv_ctrl_get_dev_info(cdev);
	unsigned dev_id = dinfo->dev_id;
	unsigned short q_id = info->qid;
	const struct ublksrv_queue *q;

	sched_setscheduler(getpid(), SCHED_RR, NULL);

	ublk_json_write_queue_info(cdev, q_id, ublksrv_gettid());
	ublk_tgt_store_dev_data(dev);

	q = ublksrv_queue_init(dev, q_id, NULL);
	if (!q) {
		fprintf(stderr, "ublk dev %d queue %d init queue failed\n",
				dinfo->dev_id, q_id);
		demo_queue_startup_wait(info, true);
		return NULL;
	}

	int ret = demo_park_fetches(q);
	if (ret < 0) {
		fprintf(stderr, "queue %d: submit initial FETCH commands failed: %d\n",
			info->qid, ret);
		ublksrv_queue_deinit(q);
		demo_queue_startup_wait(info, true);
		return NULL;
	}

	if (use_pbuf) {
		if (demo_setup_buf_pools(q, info) < 0) {
			ublksrv_queue_deinit(q);
			demo_queue_startup_wait(info, true);
			return NULL;
		}
	}

	if (!demo_queue_startup_wait(info, false)) {
		ublksrv_queue_deinit(q);
		if (use_pbuf)
			demo_teardown_buf_pools(info);
		return NULL;
	}

	fprintf(stdout, "tid %d: ublk dev %d queue %d started\n",
			ublksrv_gettid(),
			dev_id, q->q_id);
	do {
		if (ublksrv_process_io(q) < 0)
			break;
	} while (1);

	fprintf(stdout, "ublk dev %d queue %d exited\n", dev_id, q->q_id);
	ublksrv_queue_deinit(q);
	if (use_pbuf)
		demo_teardown_buf_pools(info);
	return NULL;
}

static void demo_null_set_parameters(struct ublksrv_ctrl_dev *cdev,
		const struct ublksrv_dev *dev)
 {
	const struct ublksrv_ctrl_dev_info *info =
		ublksrv_ctrl_get_dev_info(cdev);
	struct ublk_params p = {
		.types = UBLK_PARAM_TYPE_BASIC,
		.basic = {
			.logical_bs_shift	= 9,
			.physical_bs_shift	= 12,
			.io_opt_shift		= 12,
			.io_min_shift		= 9,
			.max_sectors		= info->max_io_buf_bytes >> 9,
			.dev_sectors		= dev->tgt.dev_size >> 9,
		},
	};
	int ret;

	ublk_json_write_params(cdev, &p);

	ret = ublksrv_ctrl_set_params(cdev, &p);
	if (ret)
		fprintf(stderr, "dev %d set basic parameter failed %d\n",
				info->dev_id, ret);
}

static int demo_null_io_handler(struct ublksrv_ctrl_dev *ctrl_dev)
{
	struct demo_queue_startup startup = {
		.lock = PTHREAD_MUTEX_INITIALIZER,
		.cond = PTHREAD_COND_INITIALIZER,
	};
	unsigned int nr_threads = 0;
	bool setup_failed;
	int ret, i;
	const struct ublksrv_dev *dev;
	struct demo_queue_info *info_array;
	void *thread_ret;
	const struct ublksrv_ctrl_dev_info *dinfo =
		ublksrv_ctrl_get_dev_info(ctrl_dev);

	info_array = (struct demo_queue_info *)
		calloc(sizeof(struct demo_queue_info), dinfo->nr_hw_queues);
	if (!info_array)
		return -ENOMEM;

	dev = ublksrv_dev_init(ctrl_dev);
	if (!dev) {
		free(info_array);
		return -ENOMEM;
	}

	for (i = 0; i < dinfo->nr_hw_queues; i++) {
		info_array[i].dev = dev;
		info_array[i].qid = i;
		info_array[i].startup = &startup;
		ret = pthread_create(&info_array[i].thread, NULL,
				     demo_null_io_handler_fn,
				     &info_array[i]);
		if (ret) {
			fprintf(stderr, "create queue %d thread failed: %s\n",
				i, strerror(ret));
			break;
		}
		nr_threads++;
	}

	demo_null_set_parameters(ctrl_dev, dev);

	/* Wait until queue_init and all setup-only registrations have finished. */
	pthread_mutex_lock(&startup.lock);
	if (nr_threads != dinfo->nr_hw_queues)
		startup.failed = true;
	while (startup.ready < nr_threads)
		pthread_cond_wait(&startup.cond, &startup.lock);
	setup_failed = startup.failed;
	pthread_mutex_unlock(&startup.lock);

	ret = setup_failed ? -EIO :
		ublksrv_ctrl_start_dev(ctrl_dev, getpid());

	/* Publish the START_DEV result, then let queue threads run or unwind. */
	pthread_mutex_lock(&startup.lock);
	startup.failed |= ret < 0;
	startup.release = true;
	pthread_cond_broadcast(&startup.cond);
	pthread_mutex_unlock(&startup.lock);

	if (ret >= 0) {
		ublksrv_ctrl_get_info(ctrl_dev);
		ublk_ctrl_dump(ctrl_dev);
	}

	/* wait until we are terminated */
	for (i = 0; i < nr_threads; i++)
		pthread_join(info_array[i].thread, &thread_ret);

	pthread_cond_destroy(&startup.cond);
	pthread_mutex_destroy(&startup.lock);
	ublksrv_dev_deinit(dev);

	free(info_array);

	return ret;
}

static int null_start_daemon(struct ublksrv_ctrl_dev *ctrl_dev)
{
	int ret;

	if (ublksrv_ctrl_get_affinity(ctrl_dev) < 0)
		return -1;

	ret = demo_null_io_handler(ctrl_dev);

	return ret;
}



static int demo_init_tgt(struct ublksrv_dev *dev, int type, int argc,
		char *argv[])
{
	const struct ublksrv_ctrl_dev *cdev = ublksrv_get_ctrl_dev(dev);
	const struct ublksrv_ctrl_dev_info *info = ublksrv_ctrl_get_dev_info(cdev);
	struct ublksrv_tgt_info *tgt = &dev->tgt;
	struct ublksrv_tgt_base_json tgt_json = {
		.type = type,
	};
        strcpy(tgt_json.name, "null");

	tgt_json.dev_size = tgt->dev_size = 250UL * 1024 * 1024 * 1024;
	tgt->tgt_ring_depth = info->queue_depth;
	tgt->nr_fds = 0;

	ublk_json_write_dev_info(cdev);
	ublk_json_write_target_base(cdev, &tgt_json);

	return 0;
}

static int demo_handle_io_async(const struct ublksrv_queue *q,
		const struct ublk_io_data *data)
{
	const struct ublksrv_io_desc *iod = data->iod;

	/*
	 * Force the daemon to actually pull the write buffer into its own
	 * cache so the -g (UBLK_F_NEED_GET_DATA) vs non-g comparison reflects
	 * cache-residency effects of when the kernel performed the copy, not
	 * just the extra uring_cmd round-trip.  A XOR fold across the buffer
	 * is compiler-proof against elision via the volatile sink.
	 */
	if (ublksrv_get_op(iod) == UBLK_IO_OP_WRITE) {
		const uint64_t *p = ublksrv_io_get_buf(q, data);
		size_t n = (iod->nr_sectors << 9) / sizeof(*p);
		volatile uint64_t sink = 0;
		uint64_t acc = 0;
		size_t i;

		if (!p) {
			ublksrv_complete_io(q, data->tag, -EIO);
			return 0;
		}

		for (i = 0; i < n; i++)
			acc ^= p[i];
		sink = acc;
		(void)sink;
	}

	ublksrv_complete_io(q, data->tag, iod->nr_sectors << 9);

	return 0;
}

void *null_alloc_io_buf(const struct ublksrv_queue *q, int tag, int size)
{
	return malloc(size);
}

void null_free_io_buf(const struct ublksrv_queue *q, void *buf, int tag)
{
	free(buf);
}

static int demo_pool_tier_cmp(const void *a, const void *b)
{
	const struct ublksrv_buf_pool_spec *pa = a;
	const struct ublksrv_buf_pool_spec *pb = b;

	return (pa->buf_size > pb->buf_size) -
	       (pa->buf_size < pb->buf_size);
}

/* Parse SIZE:COUNT, with optional K/M/G suffix on SIZE. */
static int demo_parse_pool_tier(const char *arg,
				struct ublksrv_buf_pool_spec *spec)
{
	const char *sep;
	char *end;
	unsigned long long size, count, multiplier = 1;

	if (!arg || !spec || !(sep = strchr(arg, ':')) || sep == arg || !sep[1])
		return -EINVAL;

	errno = 0;
	size = strtoull(arg, &end, 0);
	if (errno || end == arg)
		return -EINVAL;

	if (end != sep) {
		if (end + 1 != sep)
			return -EINVAL;
		switch (*end) {
		case 'k': case 'K': multiplier = 1ULL << 10; break;
		case 'm': case 'M': multiplier = 1ULL << 20; break;
		case 'g': case 'G': multiplier = 1ULL << 30; break;
		default: return -EINVAL;
		}
	}

	errno = 0;
	count = strtoull(sep + 1, &end, 0);
	if (errno || end == sep + 1 || *end || !count ||
	    size > UINT_MAX / multiplier || count > UINT_MAX)
		return -EINVAL;

	spec->buf_size = (unsigned int)(size * multiplier);
	spec->nr_bufs = (unsigned int)count;
	return spec->buf_size ? 0 : -EINVAL;
}

static struct ublksrv_tgt_type demo_tgt_type = {
	.name	=  "demo_null",
	.init_tgt = demo_init_tgt,
	.handle_io_async = demo_handle_io_async,
	//.alloc_io_buf = null_alloc_io_buf,
	//.free_io_buf = null_free_io_buf,
};

int main(int argc, char *argv[])
{
	struct ublksrv_dev_data data = {
		.dev_id = -1,
		.max_io_buf_bytes = DEF_BUF_SIZE,
		.nr_hw_queues = DEF_NR_HW_QUEUES,
		.queue_depth = DEF_QD,
		.tgt_type = "demo_null",
		.tgt_ops = &demo_tgt_type,
		.run_dir = ublksrv_get_pid_dir(),
		.flags = 0,
	};
	struct ublksrv_ctrl_dev *dev;
	int ret;
	static const struct option longopts[] = {
		{ "buf",		1,	NULL, 'b' },
		{ "need_get_data",	1,	NULL, 'g' },
		{ "pbuf",		0,	NULL, 'p' },
		{ "pbuf-unpinned",	0,	NULL, 'U' },
		{ "pbuf-tier",		1,	NULL, 'P' },
		{ NULL }
	};
	int opt;
	bool use_buf = false;

	while ((opt = getopt_long(argc, argv, ":bgpUP:",
				  longopts, NULL)) != -1) {
		switch (opt) {
		case 'b':
			use_buf = true;
			break;
		case 'g':
			data.flags |= UBLK_F_NEED_GET_DATA;
			break;
		case 'p':
			use_pbuf = true;
			break;
		case 'U':
			use_pbuf = true;
			use_pinned_pbuf = false;
			break;
		case 'P': {
			struct ublksrv_buf_pool_spec spec;

			if (demo_parse_pool_tier(optarg, &spec)) {
				fprintf(stderr,
					"invalid --pbuf-tier '%s' (expected SIZE:COUNT)\n",
					optarg);
				return EXIT_FAILURE;
			}
			if (!demo_pool_tiers_explicit) {
				demo_pool_tiers_explicit = true;
				demo_nr_pool_tiers = 0;
			}
			if (demo_nr_pool_tiers == UBLK_MAX_BUF_POOLS) {
				fprintf(stderr, "too many --pbuf-tier options (max %u)\n",
					UBLK_MAX_BUF_POOLS);
				return EXIT_FAILURE;
			}
			demo_pool_tiers[demo_nr_pool_tiers++] = spec;
			use_pbuf = true;
			break;
		}
		}
	}

	if (demo_pool_tiers_explicit)
		qsort(demo_pool_tiers, demo_nr_pool_tiers,
		      sizeof(demo_pool_tiers[0]), demo_pool_tier_cmp);

	if (use_pbuf) {
		data.flags |= UBLK_F_BUF_RINGS;
		if (use_pinned_pbuf)
			data.flags |= UBLK_F_PINNED_BUFS;
	}

	if (signal(SIGTERM, sig_handler) == SIG_ERR)
		error(EXIT_FAILURE, errno, "signal");
	if (signal(SIGINT, sig_handler) == SIG_ERR)
		error(EXIT_FAILURE, errno, "signal");

	if (use_buf) {
		demo_tgt_type.alloc_io_buf = null_alloc_io_buf;
		demo_tgt_type.free_io_buf = null_free_io_buf;
	}

	dev = ublksrv_ctrl_init(&data);
	if (!dev)
		error(EXIT_FAILURE, ENODEV, "ublksrv_ctrl_init");
	/* ugly, but signal handler needs this_dev */
	this_dev = dev;

	ret = ublksrv_ctrl_add_dev(dev);
	if (ret < 0) {
		error(0, -ret, "can't add dev %d", data.dev_id);
		goto fail;
	}

	ret = null_start_daemon(dev);
	if (ret < 0) {
		error(0, -ret, "can't start daemon");
		goto fail_del_dev;
	}

	ublksrv_ctrl_del_dev(dev);
	ublksrv_ctrl_deinit(dev);
	exit(EXIT_SUCCESS);

 fail_del_dev:
	ublksrv_ctrl_del_dev(dev);
 fail:
	ublksrv_ctrl_deinit(dev);

	exit(EXIT_FAILURE);
}

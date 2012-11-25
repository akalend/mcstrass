/******************************************************************************
Copyright (c) 2011-2012, Roman Arutyunyan (arut@qip.ru)
Copyright (c) 2012,      Alexandre Kalendarev (akalend@mail.ru)
All rights reserved.

Redistribution and use in source and binary forms, with or without modification, 
are permitted provided that the following conditions are met:

   1. Redistributions of source code must retain the above copyright notice, 
      this list of conditions and the following disclaimer.

   2. Redistributions in binary form must reproduce the above copyright notice, 
      this list of conditions and the following disclaimer in the documentation
	  and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE AUTHOR ''AS IS'' AND ANY EXPRESS OR IMPLIED
WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF 
MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT 
SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, 
PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR 
BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN 
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING 
IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY 
OF SUCH DAMAGE.
*******************************************************************************/

/* 
    mc_stress - Fast Benchmarking tool for memcached capability
*/

#include <string.h>
#include <netdb.h>
#include <stdio.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <netinet/ip.h>
#include <errno.h>
#include <malloc.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/time.h>
#include <malloc.h>
#include <getopt.h>
#include <signal.h>
#include <sys/epoll.h>
#include <inttypes.h>

#define MC_PORT  11211

#define MC_REQUEST_GET_FMT "get %s\r\n"
#define MC_REQUEST_SET_FMT "set %s 0 0 %d\r\n"

#define MC_DATABLOCK 0x2A2A2A2A


#define MC_REQUEST_DEBUG  0x01
#define MC_RESPONSE_DEBUG 0x02

#define MC_DEBUG_GET	0x08
#define MC_DEBUG_SET	0x04


#define INBUFSIZE 1024

#define BAD_REQUEST 0x1

#define MAX_EVENTS 256

struct econn {
	int fd;
	size_t offs;
	int flags;
};

char *outbuf;
size_t outbufsize;
char *databuf = NULL;
int datalen = 0;
struct sockaddr_in ssin;

int concurrency = 1;
int num_threads = 1;

volatile uint64_t num_requests = 0;
volatile uint64_t max_requests = 0;
volatile uint64_t good_requests = 0;
volatile uint64_t bad_requests = 0;
volatile uint64_t in_bytes = 0;
volatile uint64_t out_bytes = 0;

uint64_t ticks;

int debug = 0;

struct timeval tv, tve;

static const char short_options[] = "n:c:t:d:g:s";

static const struct option long_options[] = {
	{ "number",       1, NULL, 'n' },
	{ "concurrency",  1, NULL, 'c' },
	{ "threads",      0, NULL, 't' },
	{ "debug",        0, NULL, 'd' },
	{ "help",         0, NULL, '%' },
	{ "get",         0, NULL, 'g' },
	{ "set",         1, NULL, 's' },
	{ NULL, 0, NULL, 0 }
};

static void sigint_handler(int arg)
{
	max_requests = num_requests;
}

static void start_time()
{
	if (gettimeofday(&tv, NULL)) {
		perror("gettimeofday");
		exit(1);
	}
}

static void end_time()
{
	if (gettimeofday(&tve, NULL)) {
		perror("gettimeofday");
		exit(1);
	}
}

static void init_conn(int efd, struct econn* ec) {

	struct epoll_event evt;
	int ret;

	ec->fd = socket(AF_INET, SOCK_STREAM, 0);
	ec->offs = 0;
	ec->flags = 0;

	if (ec->fd == -1) {
		perror("socket() failed");
		exit(1);
	}

	fcntl(ec->fd, F_SETFL, O_NONBLOCK);

	ret = connect(ec->fd, (struct sockaddr*)&ssin, sizeof(ssin));

	if (ret && errno != EINPROGRESS) {
		perror("connect() failed");
		exit(1);
	}

	evt.events = EPOLLOUT;
	evt.data.ptr = ec;

	if (epoll_ctl(efd, EPOLL_CTL_ADD, ec->fd, &evt)) {
		perror("epoll_ctl");
		exit(1);
	}
}

static void* worker(void* arg) 
{
	int efd, fd, ret, nevts, n, m;
	struct epoll_event evts[MAX_EVENTS];
	char inbuf[INBUFSIZE], c;
	struct econn ecs[concurrency], *ec;
	char key[32];

	efd = epoll_create(concurrency);

	if (efd == -1) {
		perror("epoll");
		exit(1);
	}

	srand(tv.tv_usec);
	for(n = 0; n < concurrency; ++n)
		init_conn(efd, ecs + n);
	
	for(;;) {

		nevts = epoll_wait(efd, evts, sizeof(evts) / sizeof(evts[0]), -1);

		if (nevts == -1) {
			perror("epoll_wait");
			exit(1);
		}

		for(n = 0; n < nevts; ++n) {

			ec = (struct econn*)evts[n].data.ptr;

			if (ec == NULL) {
				fprintf(stderr, "fatal: NULL econn\n");
				exit(1);
			}

			if (evts[n].events & (EPOLLHUP | EPOLLERR)) {
				/* normally this should not happen */
				fprintf(stderr, "broken connection");
				exit(1);
			}
			
			if (evts[n].events & EPOLLOUT) {
				
				uint32_t crc = rand();
	
				int keylen = sprintf( key, "1.%u", crc );
				
				
				if ( debug & MC_DEBUG_GET )
					outbufsize = sprintf(outbuf, MC_REQUEST_GET_FMT, key );
				
				if (debug & MC_DEBUG_SET ) {
					outbufsize = sprintf(outbuf, MC_REQUEST_SET_FMT, key, datalen);
					memcpy(outbuf+outbufsize, databuf, datalen );
					strcpy(outbuf+outbufsize+datalen, "\r\n");
					outbufsize += datalen+2;
				}
				
				ret = send(ec->fd, outbuf + ec->offs, outbufsize - ec->offs, 0);

				if (ret == -1 && errno != EAGAIN) {
					/* TODO: something better than this */
					perror("send");
					exit(1);
				}

				if (ret > 0) {

					if (debug & MC_REQUEST_DEBUG)
						write(2, outbuf+ ec->offs, outbufsize - ec->offs);

					ec->offs += ret;

					/* write done? schedule read */
					if (ec->offs == outbufsize) {

						evts[n].events = EPOLLIN;
						evts[n].data.ptr = ec;

						ec->offs = 0;

						if (epoll_ctl(efd, EPOLL_CTL_MOD, ec->fd, evts + n)) {
							perror("epoll_ctl");
							exit(1);
						}
					}
				}

			} else if (evts[n].events & EPOLLIN) {
				
				for(;;) {
					
					ret = recv(ec->fd, inbuf, sizeof(inbuf), 0);

					if (ret == -1 && errno != EAGAIN) {
						perror("recv");
						exit(1);
					}
					
					if (ret <= 0)
						break;
	
					int off = ret - 5 > 0 ? ret - 5 : 0;
					int off2 = ret - 8 > 0 ? ret - 8 : 0;

					if (debug & MC_RESPONSE_DEBUG)
						write(2, inbuf, ret);

					//printf("buff: %s\r\nlen=%d\n", inbuf , ret);
					ec->offs += ret;

					if (ret == 7 && !strncmp(inbuf,"ERROR", 5 )) {
						ec->flags |= BAD_REQUEST;
						ret = 0;
						break;						
					}
					
					if (!strncmp(inbuf+off2, "\nERROR\r\n",8))	{
						ret = 0;
						break;
					}


					if (!strncmp(inbuf+off, "END\r\n",5))	{
						ret = 0;
						break;
					}
					
					if (!strncmp(inbuf+off2, "STORED\r\n",8))	{
						ret = 0;
						break;
					}

				}
				
				
				if (!ret) {
					close(ec->fd);

					m = __sync_fetch_and_add(&num_requests, 1);

					if (max_requests && m + 1 > max_requests)
						__sync_fetch_and_sub(&num_requests, 1);

					else if (ec->flags & BAD_REQUEST)
						__sync_fetch_and_add(&bad_requests, 1);

					else
						__sync_fetch_and_add(&good_requests, 1);

					if (max_requests && m + 1 >= max_requests) {
						end_time();
						return NULL;
					}

					if (ticks && m % ticks == 0)
						printf("%d requests\n", m);

					init_conn(efd, ec);
				}
				
			}			
		} // event cycle
	}
}

static void print_usage() 
{
	printf("Usage: htstress [options] [hostname[:port]/path\n"
			"Options:\n"
			"   -n, --number       total number of requests (0 for inifinite, Ctrl-C to abort)\n"
			"   -c, --concurrency  number of concurrent connections\n"
			"   -t, --threads      number of threads (set this to the number of CPU cores)\n"
			"   -d, --debug        debug response\n"
			"   --help             display this message\n"
			"   -g, --get          send only get\n"
			"   -s, --set=datalen  send set datablock lenght=[datalen] * 8 (--set=1 : lenght=8, 2: 16, 3 : 24 etc)\n"	
		  );
	exit(0);
}

int main(int argc, char* argv[]) 
{
	char *rq, *s;
	double delta, rps;
	int next_option;
	int n;	
	int port = MC_PORT;
	char *host = NULL;
	struct hostent *h;

	if (argc == 1)
		print_usage();

	do {
		next_option = getopt_long(argc, argv, short_options, long_options, NULL);

		switch (next_option) {

			case 'n':
				max_requests = strtoull(optarg, 0, 10);
				break;

			case 'c':
				concurrency = atoi(optarg);
				break;

			case 't':
				num_threads = atoi(optarg);
				break;

			case 'd':
				debug |= 0x03;
				break;

			case 'g':
				debug |= MC_DEBUG_GET;
				break;
				
			case 's':
				debug |= MC_DEBUG_SET;
				datalen = atoi(optarg);
				break;

				case '%':
				print_usage();

			case -1:
				break;

			default:
				printf("Unexpected argument: '%c'\n", next_option);
				return 1;
		}
	} while (next_option != -1);

	if (optind >= argc) {
		printf("Missing URL\n");
		return 1;
	}

	/* parse URL */
	s = argv[optind];

	host = s;

	rq = strpbrk(s, ":");

	if (rq == NULL)
		rq = "/";

	else if (*rq == '/') {
		host = malloc(rq - s);
		memcpy(host, rq, rq - s);

	} else if (*rq == ':') {
		*rq++ = 0;
		port = atoi(rq);
		rq = strchr(rq, '/');
		if (rq == NULL)
			rq = "/";
	}

	printf( "host=%s port=%d debug=%d threads=%d\r\n", host, port , debug, num_threads);
	
	h = gethostbyname(host);
	if (!h || !h->h_length) {
		printf("gethostbyname failed\n");
		return 1;
	}

	ssin.sin_addr.s_addr = *(u_int32_t*)h->h_addr;
	ssin.sin_family = PF_INET;
	ssin.sin_port = htons(port);


	int keylen = 12;
	
	if (debug & MC_DEBUG_SET) {
		databuf = malloc(8 * datalen);
		unsigned *p = (unsigned *)databuf;
		int i = datalen;
		while(i--) { 
			*p = MC_DATABLOCK;
			p++;
		}
		
		datalen *= sizeof(uint32_t);
		outbuf = malloc(sizeof(MC_REQUEST_SET_FMT) + keylen + datalen + 2);		
		p = (unsigned *)databuf;
		

	} else
		outbuf = malloc(sizeof(MC_REQUEST_GET_FMT) + keylen);
	
		
	ticks = max_requests / 10;

	signal(SIGINT, &sigint_handler);

	if (!max_requests) {
		ticks = 1000;
		printf("[Press Ctrl-C to finish]\n");
	}

	start_time();
	pthread_t* useless_thread = (pthread_t*)calloc(num_threads, sizeof(pthread_t));
	
	/* run test */
	if ( num_threads > 1 ) {
		for(n = 0; n < num_threads ; ++n) {
			printf("pthread_create(...)\n");
			if( pthread_create(useless_thread+n, 0, &worker, 0)){
				perror("thread create:");
			}
		}
		
		for(n = 0; n < num_threads ; ++n) {
			if( pthread_join(useless_thread[n], NULL)){
				perror("join thread:");
			}
		}
		
	} else 
		worker(0);

	/* output result */
	delta = tve.tv_sec - tv.tv_sec + ((double)(tve.tv_usec - tv.tv_usec)) / 1e6;

	printf("\n"
			"requests:      %"PRIu64"\n"
			"good requests: %"PRIu64" [%d%%]\n"
			"bad requests:  %"PRIu64" [%d%%]\n"
			"seconds:       %.3f\n"
			"requests/sec:  %.3f\n"
			"\n",
			num_requests,
			good_requests, (int)(num_requests ? good_requests * 100 / num_requests : 0),
			bad_requests, (int)(num_requests ? bad_requests * 100 / num_requests: 0),
			delta,
			delta > 0
				? max_requests / delta
				: 0
		  );

	if(databuf) free(databuf);
	free(outbuf);
	return 0;
}


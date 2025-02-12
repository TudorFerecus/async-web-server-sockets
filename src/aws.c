// SPDX-License-Identifier: BSD-3-Clause

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/sendfile.h>
#include <sys/eventfd.h>
#include <libaio.h>
#include <errno.h>

#include "aws.h"
#include "utils/util.h"
#include "utils/debug.h"
#include "utils/sock_util.h"
#include "utils/w_epoll.h"

static int listenfd;

static int epollfd;

struct io_event events[1];

static int aws_on_path_cb(http_parser *p, const char *buf, size_t len)
{
	struct connection *conn = (struct connection *)p->data;

	memcpy(conn->request_path, buf, len);
	conn->request_path[len] = '\0';
	conn->have_path = 1;

	return 0;
}

static void connection_prepare_send_reply_header(struct connection *conn)
{
	const char *resp = "200 OK";

	if (conn->state == STATE_SENDING_404 || conn->state == STATE_404_SENT) {
		dlog(LOG_INFO, "Sending 404\n");
		resp = "404 Not Found";
	}

	int header_len = snprintf(conn->send_buffer, BUFSIZ,
		"HTTP/1.1 %s\r\n"
		"\r\n",
		resp);
	conn->send_len = header_len;
	conn->send_pos = 0;
}

static enum resource_type connection_get_resource_type(struct connection *conn)
{
	if (strstr(conn->filename, AWS_REL_STATIC_FOLDER))
		return RESOURCE_TYPE_STATIC;
	if (strstr(conn->filename, AWS_REL_DYNAMIC_FOLDER))
		return RESOURCE_TYPE_DYNAMIC;
	return RESOURCE_TYPE_NONE;
}

struct connection *connection_create(int sockfd)
{
	struct connection *con = malloc(sizeof(struct connection));

	memset(con->recv_buffer, 0, BUFSIZ);
	memset(con->send_buffer, 0, BUFSIZ);
	memset(con->request_path, 0, BUFSIZ);

	con->recv_len = 0;
	con->send_len = 0;
	con->send_pos = 0;

	con->sockfd = sockfd;
	con->eventfd = eventfd(0, 0);
	con->state = STATE_INITIAL;
	return con;
}

void connection_start_async_io(struct connection *conn)
{
	int res = conn->file_size < BUFSIZ ? conn->file_size : BUFSIZ;

	dlog(LOG_INFO, "Starting async IO\n");
	conn->ctx = 0;
	conn->eventfd = eventfd(0, 0);
	conn->state = STATE_ASYNC_ONGOING;
	memset(conn->send_buffer, '\0', BUFSIZ);
	memset(&conn->iocb, 0, sizeof(conn->iocb));

	io_prep_pread(&conn->iocb, conn->fd, conn->send_buffer, res, conn->file_pos);
	conn->piocb[0] = &conn->iocb;
	io_set_eventfd(&conn->iocb, conn->eventfd);
	io_setup(1, &conn->ctx);
	io_submit(conn->ctx, 1, conn->piocb);
}

void connection_remove(struct connection *conn)
{
	dlog(LOG_INFO, "Removing connection\n");
	conn->state = STATE_CONNECTION_CLOSED;
	w_epoll_remove_ptr(epollfd, conn->sockfd, conn);

	close(conn->sockfd);
	free(conn);
}

void handle_new_connection(void)
{
	struct sockaddr_in addr;
	socklen_t len = sizeof(struct sockaddr_in);
	int fd = accept(listenfd, (struct sockaddr *)&addr, &len);

	fcntl(fd, F_SETFL, O_NONBLOCK);

	struct connection *con = connection_create(fd);

	w_epoll_add_ptr_in(epollfd, fd, con);
	http_parser_init(&con->request_parser, HTTP_REQUEST);
	con->request_parser.data = con;
}

int connection_open_file(struct connection *conn)
{
	struct stat st;

	if (stat(conn->filename, &st) < 0) {
		dlog(LOG_ERR, "stat failed:");
		conn->state = STATE_SENDING_404;
		return -1;
	}
	conn->file_size = st.st_size;
	conn->fd = open(conn->filename, O_RDONLY);
	if (conn->fd < 0) {
		dlog(LOG_ERR, "stat failed PATH");
		conn->state = STATE_SENDING_404;
		return -1;
	}
	return conn->fd;
}

void receive_data(struct connection *conn)
{
	conn->state = STATE_RECEIVING_DATA;
	int read = 1;

	while (read > 0) {
		read = recv(conn->sockfd, conn->recv_buffer + conn->recv_len, BUFSIZ - conn->recv_len, 0);
		conn->recv_len += read;
	}
	conn->state = STATE_REQUEST_RECEIVED;

	parse_header(conn);
	snprintf(conn->filename, sizeof(conn->filename), "%s%s", AWS_DOCUMENT_ROOT, conn->request_path + 1);
	conn->res_type = connection_get_resource_type(conn);
	w_epoll_update_ptr_inout(epollfd, conn->sockfd, conn);
	connection_open_file(conn);
	connection_prepare_send_reply_header(conn);
}

void connection_complete_async_io(struct connection *conn)
{
	io_destroy(conn->ctx);
	close(conn->eventfd);
}

int parse_header(struct connection *conn)
{
	http_parser_settings settings_on_path = {
		.on_message_begin = 0,
		.on_header_field = 0,
		.on_header_value = 0,
		.on_path = aws_on_path_cb,
		.on_url = 0,
		.on_fragment = 0,
		.on_query_string = 0,
		.on_body = 0,
		.on_headers_complete = 0,
		.on_message_complete = 0
	};
	size_t nparsed = http_parser_execute(&conn->request_parser, &settings_on_path, conn->recv_buffer, conn->recv_len);

	if (nparsed < conn->recv_len) {
		connection_remove(conn);
		return -1;
	}
	return 0;
}

enum connection_state connection_send_static(struct connection *conn)
{
	dlog(LOG_INFO, "sending static\n");
	int sent = sendfile(conn->sockfd, conn->fd, NULL, conn->file_size);

	dlog(LOG_INFO, "sent\n");
	if (sent < 0) {
		dlog(LOG_INFO, "remove static\n");
		close(conn->fd);
		connection_remove(conn);
		return -1;
	}
	conn->file_size -= sent;
	if (conn->file_size == 0) {
		dlog(LOG_INFO, "finish static\n");
		close(conn->fd);
		conn->state = STATE_DATA_SENT;
		w_epoll_update_ptr_in(epollfd, conn->sockfd, conn);
		return STATE_DATA_SENT;
	}
	conn->state = STATE_SENDING_DATA;
	// nu conteaza ce returnez, nimic din viata nu conteaza oricum
	return STATE_DATA_SENT;
}

int connection_send_data(struct connection *conn)
{
	dlog(LOG_INFO, "%s\n", conn->send_buffer);

	int sent = send(conn->sockfd, conn->send_buffer + conn->send_pos, conn->send_len, 0);

	if (sent < 0) {
		connection_remove(conn);
		return -1;
	}

	conn->send_len -= sent;
	conn->send_pos += sent;

	if (sent == 0 || conn->send_len <= 0) {
		if (conn->state == STATE_SENDING_404) {
			dlog(LOG_INFO, "404 HEADER sent\n");
			conn->state = STATE_404_SENT;
			return 1;
		}
		conn->state = STATE_HEADER_SENT;
		conn->send_pos = 0;
	}
	return 0;
}

void connection_init_dynamic(struct connection *conn)
{
	conn->send_pos = 0;
	io_getevents(conn->ctx, 1, 1, events, NULL);
	conn->send_len = events[0].res;
	conn->file_pos += events[0].res;
}

int connection_send_dynamic(struct connection *conn)
{
	int sent = 1;

	while (sent > 0) {
		sent = send(conn->sockfd, conn->send_buffer + conn->send_pos, conn->send_len, 0);
		if (sent < 0) {
			connection_remove(conn);
			conn->state = STATE_404_SENT;
			return -1;
		}
		conn->send_len -= sent;
		conn->file_size -= sent;
		conn->send_pos += sent;
	}
	if (conn->file_size == 0) {
		conn->state = STATE_DATA_SENT;
		connection_complete_async_io(conn);
		connection_remove(conn);
		return 0;
	}

	connection_complete_async_io(conn);
	connection_start_async_io(conn);
	return 0;
}


void handle_input(struct connection *conn)
{
	dlog(LOG_INFO, "%d", conn->state);
	switch (conn->state) {
	case STATE_INITIAL:
		receive_data(conn);
		break;
	case STATE_RECEIVING_DATA:
		receive_data(conn);
		break;
	case STATE_DATA_SENT:
		receive_data(conn);
		break;
	default:
		break;
	}
}

void handle_output(struct connection *conn)
{
	dlog(LOG_INFO, "output %d\n", conn->state);
	switch (conn->state) {
	case STATE_REQUEST_RECEIVED:
		connection_send_data(conn);
		break;
	case STATE_HEADER_SENT:
		dlog(LOG_INFO, "header sent\n");
		if (conn->res_type == RESOURCE_TYPE_STATIC) {
			connection_send_static(conn);
		} else {
			if (conn->res_type == RESOURCE_TYPE_DYNAMIC) {
				connection_start_async_io(conn);
				connection_init_dynamic(conn);
				connection_send_dynamic(conn);
				break;
			}
		}
		break;
	case STATE_ASYNC_ONGOING:
		connection_send_dynamic(conn);
		break;
	case STATE_SENDING_DATA:
		connection_send_static(conn);
		break;
	case STATE_SENDING_404:
	dlog(LOG_INFO, "404 state\n");
		connection_send_data(conn);
		break;
	default:
		dlog(LOG_ERR, "Unexpected state %d\n", conn->state);
		break;
	}
}

int main(void)
{
	epollfd = w_epoll_create();
	listenfd = socket(PF_INET, SOCK_STREAM, 0);
	int opt = 1;

	setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
	struct sockaddr_in addr = {
		.sin_family = AF_INET,
		.sin_port = htons(AWS_LISTEN_PORT),
		.sin_addr.s_addr = INADDR_ANY
	};

	bind(listenfd, (SSA *)&addr, sizeof(addr));
	listen(listenfd, 1924);

	struct epoll_event ev = {
		.events = EPOLLIN,
		.data.fd = listenfd
	};

	epoll_ctl(epollfd, EPOLL_CTL_ADD, listenfd, &ev);
	w_epoll_add_fd_in(epollfd, listenfd);

	while (1) {
		struct epoll_event rev;

		w_epoll_wait_infinite(epollfd, &rev);
		if (rev.data.fd == listenfd) {
			if (rev.events & EPOLLIN)
				handle_new_connection();
		} else {
			struct connection *conn = (struct connection *)rev.data.ptr;

			if (conn->state == STATE_ASYNC_ONGOING) {
				connection_send_dynamic(conn);
				break;
			}

			if (rev.events & EPOLLIN)
				handle_input(conn);

			if (rev.events & EPOLLOUT)
				handle_output(conn);

			if (conn->state == STATE_DATA_SENT || conn->state == STATE_404_SENT)
				connection_remove(conn);
		}
	}

	// nu mai am nimic de oferit lumii
	return 0;
}

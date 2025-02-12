# Async web server

## Made as an assignment for the Operating Systems course

Uses network sockets, epoll, async file sending, zero-copy

Most of the files are utilities provided by the assignment's team, most of my written code is in aws.c

The code works like this:
  - it opens a socket that listens to multiple connections using epoll
  - handles a new connection through the listener socket
  - processes the socket, reading its buffer and fulfilling the request
  - sends a header and a file dynamically or async


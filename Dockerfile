FROM ubuntu:bionic AS golang-zfs

# Install zfsutils
RUN apt-get update && apt-get install -y --no-install-recommends \
		zfsutils-linux \
	&& rm -rf /var/lib/apt/lists/*


FROM ubuntu:jammy AS golang-zfs

# Install zfsutils
RUN apt-get update \
    && apt-get install -y \
      software-properties-common \
      sudo \
    && add-apt-repository ppa:longsleep/golang-backports \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        golang-go \
		zfsutils-linux \
	&& rm -rf /var/lib/apt/lists/*


FROM python:3.6
MAINTAINER luoyu
RUN cp /etc/apt/sources.list /etc/apt/sources.list.bak
RUN sed -i s/deb.debian/ftp.cn.debian/g /etc/apt/sources.list
RUN mkdir /app
WORKDIR /app

RUN apt update && apt install -y --no-install-recommends --fix-missing \
    redis-server \
    vim \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*

COPY start.sh start.sh
RUN chmod +x start.sh
COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt -i https://pypi.douban.com/simple

ENV LC_ALL C.UTF-8
ENV LANGUAGE C.UTF-8
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

ENTRYPOINT ["/app/start.sh"]
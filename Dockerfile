# --- 第一阶段：构建 FFmpeg ---
FROM alpine:3.18 AS ffmpeg-builder
ARG TARGETARCH
# 建议使用固定的静态版本，避免下载失败
RUN apk add --no-cache wget xz && \
    if [ "$TARGETARCH" = "amd64" ]; then \
        wget -q https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-amd64-static.tar.xz; \
    elif [ "$TARGETARCH" = "arm64" ]; then \
        wget -q https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-arm64-static.tar.xz; \
    fi && \
    tar xf ffmpeg-release-*-static.tar.xz && \
    cp ffmpeg-*-static/ffmpeg /tmp/ffmpeg && \
    chmod +x /tmp/ffmpeg

# --- 第二阶段：运行环境 ---
FROM python:3.9-slim-bullseye
ENV TZ=Asia/Shanghai
WORKDIR /app

# 安装基础依赖
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    tzdata ca-certificates curl && \
    ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# 从构建阶段拷贝 FFmpeg
COPY --from=ffmpeg-builder /tmp/ffmpeg /usr/local/bin/ffmpeg

# 拷贝依赖并安装
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 拷贝所有源代码
COPY . .

# 创建必要目录并处理权限
# 注意：如果你的 start.sh 也是拷贝进去的，记得给执行权限
RUN mkdir -p /app/logs /app/hls /app/ip /app/rtp /app/web /app/config && \
    chmod +x /app/start.sh

EXPOSE 5020

ENV HLS_ROOT=/app/hls \
    PORT=5020 \
    CONFIG_FILE=/app/config/iptv_config.json

# 如果为了方便群晖修改文件，可以暂时注释掉 USER 1000，使用 root 运行
# USER 1000:1000

CMD ["/bin/sh", "/app/start.sh"]

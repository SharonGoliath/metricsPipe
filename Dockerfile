ARG CADC_PYTHON_VERSION=3.12
FROM opencadc/matplotlib:${CADC_PYTHON_VERSION}-slim as builder
ARG CADC_PYTHON_VERSION

RUN apt-get update --no-install-recommends && \
    apt-get install -y build-essential git libcfitsio-bin && \
    rm -rf /var/lib/apt/lists/ /tmp/* /var/tmp/*

WORKDIR /usr/src/app

ARG OPENCADC_BRANCH=main
ARG OPENCADC_REPO=opencadc

RUN pip install git+https://github.com/${OPENCADC_REPO}/caom2pipe@${OPENCADC_BRANCH}#egg=caom2pipe

RUN pip install git+https://github.com/${OPENCADC_REPO}/blank2caom2@${OPENCADC_BRANCH}#egg=blank2caom2

FROM python:${CADC_PYTHON_VERSION}-slim
WORKDIR /usr/src/app
ARG CADC_PYTHON_VERSION

COPY --from=builder /usr/local/lib/python${CADC_PYTHON_VERSION}/site-packages/ /usr/local/lib/python${CADC_PYTHON_VERSION}/site-packages/
COPY --from=builder /usr/local/bin/* /usr/local/bin/
COPY --from=builder /usr/local/.config/* /usr/local/.config/

COPY --from=builder /etc/magic /etc/magic
COPY --from=builder /etc/magic.mime /etc/magic.mime
COPY --from=builder /usr/lib/x86_64-linux-gnu/libmagic* /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/lib/file/magic.mgc /usr/lib/file/
COPY --from=builder /usr/share/misc/magic /usr/share/misc/magic
COPY --from=builder /usr/share/misc/magic.mgc /usr/share/misc/magic.mgc
COPY --from=builder /usr/share/file/magic.mgc /usr/share/file/magic.mgc

# fitsverify
COPY --from=builder /usr/lib/x86_64-linux-gnu/libcfitsio* /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/lib/x86_64-linux-gnu/libcurl-gnutls* /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/lib/x86_64-linux-gnu/libnghttp2* /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/lib/x86_64-linux-gnu/librtmp* /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/lib/x86_64-linux-gnu/libssh2* /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/lib/x86_64-linux-gnu/libpsl* /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/lib/x86_64-linux-gnu/libldap* /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/lib/x86_64-linux-gnu/liblber* /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/lib/x86_64-linux-gnu/libsasl* /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/lib/x86_64-linux-gnu/libbrotli* /usr/lib/x86_64-linux-gnu/

RUN useradd --create-home --shell /bin/bash cadcops
USER cadcops

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]

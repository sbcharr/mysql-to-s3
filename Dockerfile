FROM alpine:3.8

RUN apk add --update \
    --no-cache python3 && \
    python3 -m ensurepip && \
    rm -r /usr/lib/python*/ensurepip && \
    pip3 install --upgrade pip && \
    if [ ! -e /usr/bin/pip ]; then ln -s pip3 /usr/bin/pip ; fi && \
    if [[ ! -e /usr/bin/python ]]; then ln -sf /usr/bin/python3 /usr/bin/python; fi && \
    rm -r /root/.cache
RUN apk --no-cache add bash
RUN mkdir -p /usr/app
COPY . /usr/app
RUN chmod +x /usr/app/mysql-to-s3-command.sh
WORKDIR /usr/app
RUN pip3 install --no-cache-dir -r requirements.txt
RUN aws configure set aws_access_key_id AKIAJSK3RPORJFDODBIQ --profile squeakysimple
RUN aws configure set aws_secret_access_key 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 --profile squeakysimple
RUN aws configure set region us-east-2 --profile squeakysimple
ENV table none
ENV load_type full
ENV date 2018-12-12
ENV log_level WARNING
CMD ./mysql-to-s3-command.sh ${table} ${load_type} ${date} ${log_level}

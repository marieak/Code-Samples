FROM frolvlad/alpine-python3

ARG vpnconfig

RUN apk --no-cache add \
	openvpn

COPY . /etc/openvpn

WORKDIR /etc/openvpn

RUN pip install -r requirements.txt



FROM httpd:2.4
RUN apt-get update
RUN apt-get install -y --no-install-recommends \
python3 python3-pip python3-setuptools python3-dev gcc build-essential cmake nano \
&& \
apt-get clean && \
rm -rf /var/lib/apt/lists/*
COPY requirements.txt /opt/app/requirements.txt
WORKDIR /opt/app
RUN pip3 install -r requirements.txt --break-system-packages
COPY libossia-bookworm /opt/lib/libossia-bookworm
WORKDIR /opt/lib/libossia-bookworm
RUN mkdir build
WORKDIR /opt/lib/libossia-bookworm/build
RUN cmake .. -DOSSIA_PYTHON_ONLY=1 -DCMAKE_INSTALL_PREFIX=ossia-install -DPYTHON_LIBRARY=$(python3-config --prefix)/lib/libpython3.11.so -DPYTHON_EXECUTABLE=$(which python3) -DPYTHON_INCLUDE_DIR=$(python3-config --prefix)/include/python3.11
RUN make -j4
RUN make install
RUN pip3 install /opt/lib/libossia-bookworm/build/src/ossia-python/dist/pyossia-0+unknown-cp311-cp311-linux_x86_64.whl --break-system-packages
RUN rm -rf /opt/lib/libossia-bookworm
RUN apt-get remove gcc cmake build-essential -y
COPY ./var/www /var/www
COPY  cuems-httpd.conf /usr/local/apache2/conf/httpd.conf
COPY ssl.crt /etc/apache2/ssl/ssl.crt
COPY ssl.key /etc/apache2/ssl/ssl.key
RUN mkdir -p /var/run/apache2/
RUN mkdir /opt/app/cuems
COPY ws-server.py /opt/app/
COPY docker-entrypoint.sh /usr/local/bin
RUN chmod +x /usr/local/bin/docker-entrypoint.sh
WORKDIR /opt/app
CMD ["/usr/local/bin/docker-entrypoint.sh"]
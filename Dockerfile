FROM python:3
ADD . /code
WORKDIR /code
RUN pip install python-kucoin==0.1.8
RUN pip install boto3
ENV MARKET_COIN=BTC-ETH
ENV TARGET_COIN=DRGN-FOTA-TNC-NEO-CS
RUN echo $MARKET_COIN && echo $TARGET_COIN
CMD ["sh", "-c", "python src/controller.py --coin ${TARGET_COIN} --market ${MARKET_COIN} --platform Kucoin"]

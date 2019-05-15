FROM node:lts-alpine as builder
RUN apk --no-cache add openssl curl
WORKDIR /usr/bin
ENV KUBECTL_VERSION 1.12.3
RUN curl -LO https://storage.googleapis.com/kubernetes-release/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl
RUN chmod +x kubectl
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm i
COPY . .
RUN npm test

FROM node:lts-alpine as runner
COPY --from=builder /app/index.js /app/package.json /app/
COPY --from=builder /app/src /app/src
COPY --from=builder /app/node_modules /app/node_modules
COPY --from=builder /usr/bin/kubectl /usr/bin/kubectl
WORKDIR /app
USER 1000
CMD npm start

FROM node:alpine as builder
RUN apk --no-cache add openssl
WORKDIR /usr/bin
RUN wget https://storage.googleapis.com/kubernetes-release/release/$(wget -q -O - https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/linux/amd64/kubectl
RUN chmod +x kubectl
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm i
COPY . .
RUN npm test
# RUN npm prune --production

FROM node:alpine as runner
COPY --from=builder /app/index.js /app/package.json /app/
COPY --from=builder /app/src /app/src
COPY --from=builder /app/node_modules /app/node_modules
COPY --from=builder /usr/bin/kubectl /usr/bin/kubectl
WORKDIR /app
USER 1000
CMD kubectl proxy --port=8181 && npm start

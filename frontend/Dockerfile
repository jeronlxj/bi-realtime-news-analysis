FROM node:23
WORKDIR /app

# Copy package files first for better caching
COPY package*.json ./
RUN npm install

# Copy the rest of the application
COPY . .

ENV HOST=0.0.0.0
CMD ["npm", "start"]
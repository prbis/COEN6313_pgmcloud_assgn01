# Use Node.js image
FROM node:18-slim

# Create app directory inside the container
WORKDIR /usr/src/app

# Copy package.json and install dependencies
COPY package*.json ./
RUN npm install

# Copy the entire application (including the protos folder)
COPY . .

# Expose the gRPC server port
EXPOSE 50051

# Start the server
CMD ["npm", "start"]

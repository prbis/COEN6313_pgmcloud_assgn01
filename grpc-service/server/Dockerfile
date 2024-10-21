# Use Node.js image
FROM node:18-slim

# Create app directory inside the container
WORKDIR /usr/src/app

# Copy package.json and install dependencies
COPY package*.json ./
RUN npm install

# Copy the entire application (including the protos folder)
COPY . .

# Set environment variable for the port
ENV PORT=8080

# Expose the port that Cloud Run uses
EXPOSE 8080

# Start the server
CMD ["npm", "start"]

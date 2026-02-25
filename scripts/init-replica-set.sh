#!/bin/bash
# Wait for MongoDB to be ready, then initiate replica set
echo "Waiting for MongoDB to start..."
sleep 10

echo "Initiating replica set..."
mongosh -u admin -p jadc2secret --authenticationDatabase admin --eval '
  try {
    rs.status();
    print("Replica set already initiated");
  } catch(e) {
    rs.initiate({_id: "rs0", members: [{_id: 0, host: "mongodb:27017"}]});
    print("Replica set initiated successfully");
  }
'

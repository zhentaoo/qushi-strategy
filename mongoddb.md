




use admin

db.createUser({
  user: "admin",
  pwd: "wwwxxxdskjkl123990",
  roles: [{ role: "root", db: "admin" }]
})


db.auth("admin", "wwwxxxdskjkl123990")


 mongod --dbpath /usr/local/var/mongodb
 --logpath /usr/local/var/log/mongodb/mongod.log
 --auth &

 mongod --dbpath /usr/local/var/mongodb --logpath /usr/local/var/log/mongodb/mongod.log --auth &
 mongod --dbpath /usr/local/var/mongodb --logpath /usr/local/var/log/mongodb/mongod.log --auth --fork


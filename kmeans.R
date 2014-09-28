library("rmongodb")
mongo <- mongo.create(host = "166.111.135.27:27017")
cursor <- mongo.find(mongo, 'analysis2.sessions', limit = 0L)
data <- matrix(0, nrow = 629143, ncol = 2500)
mongo.cursor.next(cursor)
session <- mongo.bson.to.Robject(mongo.cursor.value(cursor))[["matrix"]]
data[1,] <- matrix(session[1:50,1:50], nrow = 1, byrow = T)
p <- 1
print(p)
while (mongo.cursor.next(cursor)) {
    p <- p + 1
    session <- mongo.bson.to.Robject(mongo.cursor.value(cursor))[["matrix"]]
    data[p,] <- matrix(session[1:50,1:50], nrow = 1, byrow = T)
#    data <- rbind(data, item)
    print(p)
}
mongo.cursor.destroy(cursor)
cl <- kmeans(data, 5)
cluster <- cl$cluster
centers <- cl$centers
totss <- cl$totss
withinss <- cl$withinss
tot.withinss <- cl$tot.withinss
betweenss <- cl$betweenss
size <- cl$size
iter <- cl$iter
ifault <- cl$ifault
write.table(cluster, file = "cluster.txt")
write.table(centers, file = "centers.txt")
write.table(totss, file = "totss.txt")
write.table(withinss, file = "withinss.txt")
write.table(tot.withinss, file = "tot.withinss.txt")
write.table(betweenss, file = "betweenss.txt")
write.table(size, file = "size.txt")
write.table(iter, file = "iter.txt")
write.table(ifault, file = "ifault.txt")
#plot(x, col = cl$cluster)
#centers <- cl$centers
#write.table(centers, file = "centers.txt")
#part1 <- matrix(centers[1,], nrow = 77, ncol = 77, byrow = T)
#for (i in 1:77) {
#    part1[,i] <- part1[,i] / sum(part1[,i])
#}
#png(file="part1.png", bg="transparent")
#image(1:77, 1:77, part1, col = topo.colors(1000), main = 'part1')
#dev.off()
#part1 <- matrix(centers[2,], nrow = 77, ncol = 77, byrow = T)
#png(file="part2.png", bg="transparent")
#image(1:77, 1:77, part1, col = topo.colors(1000), main = 'part2')
#dev.off()
#part1 <- matrix(centers[3,], nrow = 77, ncol = 77, byrow = T)
#png(file="part3.png", bg="transparent")
#image(1:77, 1:77, part1, col = topo.colors(1000), main = 'part3')
#dev.off()
#print(cl$size)

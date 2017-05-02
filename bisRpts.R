# CONNECTION TO ORACLE

library(RODBC)

channel <- odbcConnect("OracleInstantClient", uid = "zhangj", pwd = "zhangj1234")

risk.data.all <- sqlQuery(channel, 'select * from THBL.RISK_STATISTICS_ALL')
coding <- sqlQuery(channel, 'select * from THBL.RISK_DIMENSION')
# risk.data.ex <- sqlQuery(channel, 'select * from THBL.RISK_STATISTICS_EX')

risk.data.all$DATA_DT <- as.Date(risk.data.all$DATA_DT)

odbcClose(channel)

# data for test preparation
data.length = 50
lastDay <- seq(as.Date("2016-02-01"), length = 15, by = "1 month") - 1
DATA_DT <- as.character(lastDay[sample(1:length(lastDay), size = data.length, replace = T)])
BEGIN_DATE <- as.character(sample(seq(as.Date('2016/01/01'), as.Date('2017/03/31'), by = "day"), data.length))

RELOAN <- runif(data.length, 1, 10)
CNT <- floor(runif(data.length, 1, 1000))
BAL <- runif(data.length, 1, 10000)
OD_AMT <- runif(data.length, 1, 10000)
AP_AMT <- runif(data.length, 1, 10000)
SP_AMT <- runif(data.length, 1, 10000)
NEW_AMT <- runif(data.length, 1, 10000)
OD_PRINCIPAL <- runif(data.length, 1, 10000)

type.five <- c(0:4)
type.four <- c(0:3)
type.three <- c(0:2)
type.two <- c(0:1)
type.minus <- c(-1:3)

OVERDUE_STATUS_5 <- type.five[sample(1:length(type.five), size = data.length, replace = T)]
OVERDUE_STATUS_5_LAST <- type.five[sample(1:length(type.five), size = data.length, replace = T)]

OVERDUE_STATUS_3 <- type.three[sample(1:length(type.three), size = data.length, replace = T)]
OVERDUE_STATUS_3_LAST <- type.three[sample(1:length(type.three), size = data.length, replace = T)]

NEW_LOAN <- type.two[sample(1:length(type.two), size = data.length, replace = T)]

STATUS_LAST_MONTH <- type.three[sample(1:length(type.three), size = data.length, replace = T)]
STATUS_THIS_MONTH <- type.three[sample(1:length(type.three), size = data.length, replace = T)]

MATURITY_DAYS <- type.four[sample(1:length(type.minus), size = data.length, replace = T)]

OVERDUE_DAYS <- type.five[sample(1:length(type.five), size = data.length, replace = T)]

OVERDUE_FLAG <- type.two[sample(1:length(type.two), size = data.length, replace = T)]

data.test <- data.frame(DATA_DT, OVERDUE_STATUS_5, OVERDUE_STATUS_5_LAST, OVERDUE_STATUS_3, OVERDUE_STATUS_3_LAST,
                 NEW_LOAN, RELOAN, STATUS_LAST_MONTH, STATUS_THIS_MONTH, MATURITY_DAYS, OVERDUE_DAYS, OVERDUE_FLAG,
                 BEGIN_DATE, CNT, BAL, OD_AMT, AP_AMT, SP_AMT, NEW_AMT, OD_PRINCIPAL)


library(dplyr)
library(tidyr)
library(lubridate)
library(sca)

# 资金变化 Output: table.1.cnt; table.1.amt
# part.1 cnt
data.table.1 <- risk.data.all[c("DATA_DT", "OVERDUE_STATUS_3", "OVERDUE_STATUS_3_LAST","NEW_LOAN","CNT","OD_AMT","STATUS_THIS_MONTH","STATUS_LAST_MONTH", "DIFF_OD_AMT")]


table.1.cnt.part.1 <- arrange(aggregate(CNT ~ DATA_DT + OVERDUE_STATUS_3,
                                        subset(data.table.1, NEW_LOAN == 1 & OVERDUE_STATUS_3 != 2), FUN = "sum"), DATA_DT) %>%
  unite(unite.name, OVERDUE_STATUS_3, sep = "-") %>%
  reshape(idvar = "DATA_DT", timevar = "unite.name", direction = "wide")

table.1.cnt.part.2 <- arrange(aggregate(CNT ~ DATA_DT + OVERDUE_STATUS_3_LAST + OVERDUE_STATUS_3,
                                        subset(data.table.1, OVERDUE_STATUS_3_LAST != 2), FUN = "sum"), DATA_DT, OVERDUE_STATUS_3_LAST, OVERDUE_STATUS_3) %>%
  unite(unite.name, OVERDUE_STATUS_3_LAST, OVERDUE_STATUS_3, sep = "-") %>%
  reshape(idvar = "DATA_DT", timevar = "unite.name", direction = "wide")

data.part.3 <- subset(data.table.1, STATUS_LAST_MONTH == 0 & STATUS_THIS_MONTH == 1)
table.1.cnt.part.3 <- aggregate(data.part.3$CNT, list(DATA_DT = data.part.3$DATA_DT), FUN = "sum")

# table.1 cnt index
table.1.cnt <- Reduce(function(x,y) merge(x,y, by = "DATA_DT", all = T), list(table.1.cnt.part.1, table.1.cnt.part.2, table.1.cnt.part.3))
table.1.cnt[is.na(table.1.cnt)] <- 0
table.1.cnt$DIFF <- table.1.cnt$`CNT.1-1` - table.1.cnt$x
table.1.cnt$DATA_DT <- paste(month(as.Date(table.1.cnt$DATA_DT) %m-% months(1)), month(table.1.cnt$DATA_DT), sep = "-") %>%
  paste(c("汇总"), sep = "")


# part.2 amt
table.1.amt.part.1 <- arrange(aggregate(OD_AMT ~ DATA_DT + NEW_LOAN + OVERDUE_STATUS_3,
                                        subset(data.table.1, NEW_LOAN == 1 & OVERDUE_STATUS_3 != 2), FUN = "sum"), DATA_DT) %>%
  unite(unite.name, NEW_LOAN, OVERDUE_STATUS_3, sep = "-") %>%
  reshape(idvar = "DATA_DT", timevar = "unite.name", direction = "wide")

table.1.amt.part.2 <- arrange(aggregate(DIFF_OD_AMT ~ DATA_DT + OVERDUE_STATUS_3_LAST + OVERDUE_STATUS_3,
                                        subset(data.table.1, OVERDUE_STATUS_3_LAST != 2), FUN = "sum"), DATA_DT, OVERDUE_STATUS_3_LAST, OVERDUE_STATUS_3) %>%
  unite(unite.name, OVERDUE_STATUS_3_LAST, OVERDUE_STATUS_3, sep = "-") %>%
  reshape(idvar = "DATA_DT", timevar = "unite.name", direction = "wide")

table.1.amt.part.3 <- aggregate(data.part.3$DIFF_OD_AMT, list(DATA_DT = data.part.3$DATA_DT), FUN = "sum")

# table.1 amt index
table.1.amt <- Reduce(function(x,y) merge(x,y, by = "DATA_DT", all = T), list(table.1.amt.part.1, table.1.amt.part.2, table.1.amt.part.3))
# !!!!!!names
table.1.amt[is.na(table.1.amt)] <- 0
table.1.amt$DIFF <- table.1.amt$`DIFF_OD_AMT.1-1` - table.1.amt$x 
  
# sum amt 
table.1.amt$DIFFAMT <- apply(table.1.amt[2:9], 1, sum)
table.1.amt$DATA_DT <- paste(month(as.Date(table.1.amt$DATA_DT) %m-% months(1)), month(table.1.amt$DATA_DT), sep = "-") %>%
  paste(c("汇总"), sep = "")


# 续贷逾期情况 Output: table.2
# reloan == 1
data.table.2 <- arrange(risk.data.all[c("DATA_DT", "RELOAN", "OVERDUE_STATUS_3", "CNT", "BAL", "OD_AMT", "MATURITY_DAYS")], DATA_DT)

table.2.part.1.firstLoan <- aggregate(CNT~DATA_DT, subset(data.table.2, RELOAN == 1 & OVERDUE_STATUS_3 != 2), FUN = "sum")

table.2.part.2.firstLoan <- aggregate(CNT~DATA_DT, subset(data.table.2, RELOAN == 1 & OVERDUE_STATUS_3 == 1), FUN = "sum")

table.2.part.3.firstLoan <- aggregate(BAL~DATA_DT, subset(data.table.2, RELOAN == 1), FUN = "sum")

table.2.part.4.firstLoan <- aggregate(OD_AMT~DATA_DT, subset(data.table.2, RELOAN == 1), FUN = "sum")

table.2.firstLoan <- Reduce(function(x,y) merge(x,y, by = "DATA_DT", all = T), list(table.2.part.1.firstLoan, table.2.part.2.firstLoan,
                                                               table.2.part.3.firstLoan, table.2.part.4.firstLoan))
# !!!!!!精度控制
table.2.firstLoan$Ratio <-  percent(table.2.firstLoan$OD_AMT/table.2.firstLoan$BAL, d= 2)

# reloan > 1
table.2.part.1.reLoan <- aggregate(CNT~DATA_DT, subset(data.table.2, RELOAN != 1 & OVERDUE_STATUS_3 != 2), FUN = "sum")

table.2.part.2.reLoan <- aggregate(CNT~DATA_DT, subset(data.table.2, RELOAN != 1 & OVERDUE_STATUS_3 == 1), FUN = "sum")

table.2.part.3.reLoan <- aggregate(BAL~DATA_DT, subset(data.table.2, RELOAN != 1), FUN = "sum")

table.2.part.4.reLoan <- aggregate(OD_AMT~DATA_DT, subset(data.table.2, RELOAN != 1), FUN = "sum")

table.2.reLoan <- Reduce(function(x,y) merge(x,y, by = "DATA_DT", all = T), list(table.2.part.1.reLoan, table.2.part.2.reLoan,
                                                                                    table.2.part.3.reLoan, table.2.part.4.reLoan))
# !!!!!!精度控制
table.2.reLoan$Ratio <- percent(table.2.reLoan$OD_AMT/table.2.reLoan$BAL, d = 2)

# all
table.2.part.1.allLoan <- aggregate(CNT~DATA_DT, subset(data.table.2, OVERDUE_STATUS_3 != 2), FUN = "sum")

table.2.part.2.allLoan <- aggregate(CNT~DATA_DT, subset(data.table.2, OVERDUE_STATUS_3 == 1), FUN = "sum")

table.2.part.3.allLoan <- aggregate(OD_AMT~DATA_DT, data.table.2, FUN = "sum")

table.2.part.4.allLoan <- aggregate(OD_AMT~DATA_DT, subset(data.table.2, MATURITY_DAYS > 0), FUN = "sum")

table.2.part.5.allLoan <- aggregate(OD_AMT~DATA_DT, subset(data.table.2, MATURITY_DAYS == 3), FUN = "sum")

table.2.part.6.allLoan <- aggregate(BAL~DATA_DT, data.table.2, FUN = "sum")

table.2.allLoan <- Reduce(function(x,y) merge(x,y, by = "DATA_DT", all = T), list(table.2.part.1.allLoan, table.2.part.2.allLoan,
                                                                                  table.2.part.3.allLoan, table.2.part.4.allLoan,
                                                                                  table.2.part.5.allLoan, table.2.part.6.allLoan))
table.2.allLoan$ODRatio <- percent(table.2.allLoan$OD_AMT.x/table.2.allLoan$BAL, d= 2)
table.2.allLoan$BORatio <- percent(table.2.allLoan$OD_AMT.y/table.2.allLoan$BAL, d = 2)
table.2.allLoan$BNRatio <- percent(table.2.allLoan$OD_AMT/table.2.allLoan$BAL, d = 2)

table.2 <- Reduce(function(x,y) merge(x,y, by = "DATA_DT", all = T), list(table.2.firstLoan, table.2.reLoan, table.2.allLoan))

table.2$DATA_DT <- substr(table.2$DATA_DT, 1, 7)

# 还款情况 Output: table.3
data.table.3 <- arrange(risk.data.all[c("DATA_DT", "SP_AMT", "AP_AMT", "OVERDUE_STATUS_3")], DATA_DT)

table.3.part.1 <- aggregate(SP_AMT~DATA_DT, data.table.3, FUN = "sum")
table.3.part.2 <- aggregate(AP_AMT~DATA_DT, data.table.3, FUN = "sum")
table.3.part.3 <- aggregate(SP_AMT~DATA_DT, subset(data.table.3, OVERDUE_STATUS_3 == 1), FUN = "sum")
table.3.part.4 <- aggregate(AP_AMT~DATA_DT, subset(data.table.3, OVERDUE_STATUS_3 == 1), FUN = "sum")

table.3 <- Reduce(function(x,y) merge(x,y, by = "DATA_DT", all = T), list(table.3.part.1, table.3.part.2,
                                                                          table.3.part.3, table.3.part.4))

# 迁徙率月度情况 Output: table.4.1; table.4.2
data.table.4 <- arrange(risk.data.all[c("DATA_DT", "OVERDUE_STATUS_5", "OVERDUE_STATUS_5_LAST" ,"CNT")], DATA_DT)

table.4.1 <- arrange(aggregate(data.table.4$CNT, by = list(DATA_DT = DATA_DT, OVERDUE_STATUS_5 = OVERDUE_STATUS_5), FUN = "sum") %>%
  reshape(idvar = "DATA_DT", timevar = "OVERDUE_STATUS_5", direction = "wide"), DATA_DT)
table.4.1[is.na(table.4.1)] <- 0
table.4.1$regular <- table.4.1$x.0 + table.4.1$x.1


data.table.4.2 <- na.omit(subset(data.table.4, (OVERDUE_STATUS_5_LAST == 0 | OVERDUE_STATUS_5_LAST == 1) &
                           (OVERDUE_STATUS_5 != 0 & OVERDUE_STATUS_5 != 1)))
table.4.part.2 <- arrange(setNames(aggregate(data.table.4.2$CNT, data.table.4.2["DATA_DT"], FUN = "sum"),
                                   c("DATA_DT","0-1")), DATA_DT)

data.table.4.3 <- na.omit(subset(data.table.4, OVERDUE_STATUS_5_LAST == 2 & OVERDUE_STATUS_5 == 3))
table.4.part.3 <- arrange(setNames(aggregate(data.table.4.3$CNT, data.table.4.3["DATA_DT"], FUN = "sum"),
                                   c("DATA_DT","2-3")), DATA_DT)

data.table.4.4 <- na.omit(subset(data.table.4, OVERDUE_STATUS_5_LAST == 2 & OVERDUE_STATUS_5 == 4))
table.4.part.4 <- arrange(setNames(aggregate(data.table.4.4$CNT, data.table.4.4["DATA_DT"], FUN = "sum"),
                                   c("DATA_DT","2-4")), DATA_DT)

data.table.4.5 <- na.omit(subset(data.table.4, OVERDUE_STATUS_5_LAST == 3 & OVERDUE_STATUS_5 == 4))
table.4.part.5 <- arrange(setNames(aggregate(data.table.4.5$CNT, data.table.4.5["DATA_DT"], FUN = "sum"),
                                   c("DATA_DT","3-4")), DATA_DT)

data.table.4.6 <- na.omit(subset(data.table.4, OVERDUE_STATUS_5_LAST == 4 & OVERDUE_STATUS_5 == 4))
table.4.part.6 <- arrange(setNames(aggregate(data.table.4.6$CNT, data.table.4.6["DATA_DT"], FUN = "sum"),
                                   c("DATA_DT","4-4")), DATA_DT)

table.4.2 <- Reduce(function(x,y) merge(x,y, by = "DATA_DT", all = T), list(table.4.part.2,
                                                                          table.4.part.3, table.4.part.4,
                                                                          table.4.part.5, table.4.part.6))
table.4.1[is.na(table.4.part.1)] <- 0
table.4.2[is.na(table.4.2)] <- 0

table.4.2$rate.1 <- table.4.2$`0-1`/table.4.part.1$regular[-nrow(table.4.part.1)]
table.4.2$rate.2 <- table.4.2$`2-3`/table.4.part.1$x.2[-nrow(table.4.part.1)]
table.4.2$rate.3 <- table.4.2$`2-4`/table.4.part.1$x.2[-nrow(table.4.part.1)]
table.4.2$rate.4 <- table.4.2$`3-4`/table.4.part.1$x.3[-nrow(table.4.part.1)]
table.4.2$rate.5 <- table.4.2$`4-4`/table.4.part.1$x.4[-nrow(table.4.part.1)]


# 资产情况月度分析  Output: table.5.part.1; table.5.part.2; table.5.part.3
data.table.5 <- arrange(risk.data.all[c("DATA_DT", "OVERDUE_STATUS_3", "OD_AMT", "BEGIN_DATE", "RELOAN", "NEW_AMT")], DATA_DT)
data.table.5$BEGIN_DATE <- substr(data.table.5$BEGIN_DATE, 1, 7)

table.5.part.1.1 <- arrange(aggregate(NEW_AMT~DATA_DT, data.table.5, FUN = "sum"), DATA_DT)
table.5.part.1.2 <- arrange(aggregate(OD_AMT~DATA_DT+BEGIN_DATE, data.table.5, FUN = "sum"), DATA_DT, BEGIN_DATE) %>%
  reshape(idvar = "DATA_DT", timevar = "BEGIN_DATE", direction = "wide")
table.5.part.1 <- merge(table.5.part.1.1, table.5.part.1.2, all = T)

table.5.part.2 <- cbind(table.5.part.1[,1:2], table.5.part.1[,-1:-2]/table.5.part.1$NEW_AMT)

table.5.part.3.1 <- arrange(aggregate(NEW_AMT~DATA_DT, subset(data.table.5, RELOAN > 1), FUN = "sum"), DATA_DT)
table.5.part.3.2 <- arrange(aggregate(OD_AMT~DATA_DT+BEGIN_DATE, subset(data.table.5, RELOAN > 1), FUN = "sum"), DATA_DT) %>%
  reshape(idvar = "DATA_DT", timevar = "BEGIN_DATE", direction = "wide")
table.5.part.3 <- merge(table.5.part.3.1, table.5.part.3.2, all = T)
table.5.part.3 <- cbind(table.5.part.3[,1:2], table.5.part.3[,-1:-2]/table.5.part.3$NEW_AMT)

# 终止和到期情况 Output: table.6.all; table.6.ex
# 到期？？？MATURITY_DAYS
index.table.6 <- function(x){
  data.table.6 <- arrange(x[c("DATA_DT", "OVERDUE_STATUS_3", "CNT", "OD_AMT", "MATURITY_DAYS", "STATUS_THIS_MONTH")], DATA_DT)
  
  table.6.part.1 <- arrange(aggregate(CNT~DATA_DT, subset(data.table.6, OVERDUE_STATUS_3 == 1), FUN = "sum"), DATA_DT)
  table.6.part.2 <- arrange(aggregate(OD_AMT~DATA_DT, subset(data.table.6, OVERDUE_STATUS_3 == 1), FUN = "sum"), DATA_DT)
  table.6.part.3 <- arrange(aggregate(CNT~DATA_DT, subset(data.table.6, MATURITY_DAYS != -1), FUN = "sum"), DATA_DT)
  table.6.part.4 <- arrange(aggregate(OD_AMT~DATA_DT, subset(data.table.6, MATURITY_DAYS != -1), FUN = "sum"), DATA_DT)
  table.6.part.5 <- arrange(aggregate(CNT~DATA_DT, subset(data.table.6, STATUS_THIS_MONTH == 1), FUN = "sum"), DATA_DT)
  table.6.part.6 <- arrange(aggregate(OD_AMT~DATA_DT, subset(data.table.6, STATUS_THIS_MONTH == 1), FUN = "sum"), DATA_DT)
  table.6 <- Reduce(function(x,y) merge(x,y, by = "DATA_DT", all = T), list(table.6.part.1, table.6.part.2, table.6.part.3,
                                                                            table.6.part.4, table.6.part.5, table.6.part.6))
}
table.6.all <- index.table.6(risk.data.all)
table.6.ex <- index.table.6(risk.data.ex)


# 资产结构类数据 Output: table.7.1; table.7.2; table.7.3
table.7.part.1 <- table.4.part.1
table.7.part.1$sum <- apply(table.7.part.1[,2:6], 1, sum)
table.7.part.1$type0 <- table.7.part.1$regular/table.7.part.1$sum
table.7.part.1$type1 <- table.7.part.1$x.2/table.7.part.1$sum
table.7.part.1$type2 <- table.7.part.1$x.3/table.7.part.1$sum
table.7.part.1$type3 <- table.7.part.1$x.4/table.7.part.1$sum
table.7.1 <- table.7.part.1[,-2:-8]

data.table.7 <- risk.data.all[c("DATA_DT", "OD_AMT", "OVERDUE_STATUS_3")]

table.7.part.2 <- arrange(aggregate(OD_AMT~DATA_DT+OVERDUE_STATUS_3, data.table.7, FUN = "sum"), DATA_DT, OVERDUE_STATUS_3) %>%
  reshape(idvar = "DATA_DT", timevar = "OVERDUE_STATUS_3", direction = "wide")
table.7.part.2[is.na(table.7.part.2)] <- 0
table.7.part.2$sum <- apply(table.7.part.2[,2:4], 1, sum)
table.7.part.2$type0 <- table.7.part.2$OD_AMT.0/table.7.part.2$sum
table.7.part.2$type1 <- table.7.part.2$OD_AMT.1/table.7.part.2$sum
table.7.part.2$type2 <- table.7.part.2$OD_AMT.2/table.7.part.2$sum
table.7.2 <- table.7.part.2[, -2:-5]

data.table.7.3 <- arrange(risk.data.all[c("DATA_DT", "OVERDUE_STATUS_3", "OVERDUE_STATUS_5", "CNT", "SP_AMT", "OD_PRINCIPAL", "OD_AMT", "BAL")] %>%
  subset(OVERDUE_STATUS_3 != 2 & as.Date(DATA_DT) >= as.Date('2017-01-01')), DATA_DT)

combined.1 <- subset(data.table.7.3, OVERDUE_STATUS_5 == 0 | OVERDUE_STATUS_5 == 1)
combined.1 <- aggregate(combined.1[,4:8], by = combined.1["DATA_DT"], sum)
combined.1 <- as.data.frame(append(combined.1, list(OVERDUE_STATUS_5 = 0), after = 1))

combined.2 <- subset(data.table.7.3, OVERDUE_STATUS_5 != 0 & OVERDUE_STATUS_5 != 1)
combined.2 <- aggregate(combined.2[,4:8], by = list(DATA_DT = combined.2$DATA_DT, OVERDUE_STATUS_5 = combined.2$OVERDUE_STATUS_5), sum)

data.table.7.3 <- arrange(rbind(combined.1, combined.2), DATA_DT)
data.table.7.3.sumCNT <-setNames(aggregate(CNT~DATA_DT, data.table.7.3, FUN = "sum"), c("DATA_DT", "sumCNT"))
data.table.7.3.sumAMT <- setNames(aggregate(BAL~DATA_DT, data.table.7.3, FUN = "sum"), c("DATA_DT", "sumAMT"))
data.table.7.3 <- Reduce(function(x,y) merge(x,y, by = "DATA_DT", all = T), list(data.table.7.3,
                                                                                 data.table.7.3.sumCNT,
                                                                                 data.table.7.3.sumAMT))
data.table.7.3 <- as.data.frame(append(data.table.7.3, list(Ratio.CNT = data.table.7.3$CNT/data.table.7.3$sumCNT), after = 3))
data.table.7.3 <- as.data.frame(append(data.table.7.3, list(Ratio.AMT = data.table.7.3$BAL/data.table.7.3$sumAMT), after = 8))

table.7.part.3.1  <- arrange(aggregate(CNT~DATA_DT+OVERDUE_STATUS_5, data.table.7.3, FUN = "sum"), DATA_DT)
table.7.part.3.1[is.na(table.7.part.3.1)] <- 0
table.7.part.3.2 <- arrange(aggregate(Ratio.CNT~DATA_DT+OVERDUE_STATUS_5, data.table.7.3, FUN = "sum"), DATA_DT)
table.7.part.3.3 <- arrange(aggregate(SP_AMT~DATA_DT+OVERDUE_STATUS_5, data.table.7.3, FUN = "sum"), DATA_DT)
table.7.part.3.4 <- arrange(aggregate(OD_PRINCIPAL~DATA_DT+OVERDUE_STATUS_5, data.table.7.3, FUN = "sum"), DATA_DT)
table.7.part.3.5 <- arrange(aggregate(OD_AMT~DATA_DT+OVERDUE_STATUS_5, data.table.7.3, FUN = "sum"), DATA_DT)
table.7.part.3.6 <- arrange(aggregate(BAL~DATA_DT+OVERDUE_STATUS_5, data.table.7.3, FUN = "sum"), DATA_DT)
table.7.part.3.6[is.na(table.7.part.3.5)] <- 0
table.7.part.3.7 <- arrange(aggregate(Ratio.AMT~DATA_DT+OVERDUE_STATUS_5, data.table.7.3, FUN = "sum"), DATA_DT)

table.7.3 <- Reduce(function(x,y) merge(x,y, all = T), list(table.7.part.3.1, table.7.part.3.2, table.7.part.3.3, table.7.part.3.4,
                                                                          table.7.part.3.5, table.7.part.3.6, table.7.part.3.7))

# OUTPUT
library(xlsx)
write.xlsx(table.1.cnt, file = "E:\\Allinpay\\Data\\TeamWork\\dataForReport\\output.xlsx", sheetName = "资金变化", row.names = F)

addDataFrame(newTable, sheet = newSheet,
             row.names = F, startRow = X)
saveWorkbook(newSheet, "filepath")



# template
library(XLConnect)
ExcelFile <- "E:\\Allinpay\\Data\\TeamWork\\dataForReport\\templateForBR.xls"
template <- paste0("E:\\Allinpay\\Data\\TeamWork\\dataForReport\\bisRpts", Sys.Date(), ".xls")
file.copy(ExcelFile, template)

# output to excel
writeWorksheetToFile(template, data = table.1.cnt,
                     sheet = "资金变化", header = F,
                     startRow = 2, startCol = 1)
writeWorksheetToFile(template, data = c("金额"),
                     sheet = "资金变化", header = F,
                     startRow = nrow(table.1.cnt) + 2, startCol = 1)
writeWorksheetToFile(template, data = table.1.amt,
                     sheet = "资金变化", header = F,
                     startRow = nrow(table.1.cnt) + 3, startCol = 1)
writeWorksheetToFile(template, data = table.2,
                     sheet = "续贷逾期情况", header = F,
                     startRow = 4, startCol = 2)
writeWorksheetToFile(template, data = table.3,
                     sheet = "还款情况", header = F,
                     startRow = 3, startCol = 3)








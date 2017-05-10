# CONNECTION TO ORACLE

library(RODBC)
library(dplyr)
library(tidyr)
library(lubridate)
library(sca)
library(XLConnect)

channel <- odbcConnect("OracleInstantClient", uid = "zhangj", pwd = "zhangj1234")

risk.data.all <- sqlQuery(channel, 'select * from THBL.RISK_STATISTICS_ALL
                          where prov_cd not like \'35%\'')
coding <- sqlQuery(channel, 'select * from THBL.RISK_DIMENSION')
# data.11.1 <- sqlQuery(channel, 'SELECT DATA_DT,
#                       COUNT(DISTINCT MCHT_CD) AS RELOAN_MCHT_CNT 
#                       FROM THBL.TEMP_RISK_STAT_LAST 
#                       where reloan > 1 
#                       GROUP BY DATA_DT')
# data.11.2 <- sqlQuery(channel, 'SELECT DATA_DT,
#                       COUNT(DISTINCT MCHT_CD) AS FINISHED_MCHT
#                       FROM THBL.TEMP_RISK_STAT_LAST 
#                       where overdue_status_3 = 2  
#                       GROUP BY DATA_DT')
# data.11.3 <- sqlQuery(channel, 'SELECT DATA_DT, PROV_CD,
#                       COUNT(DISTINCT MCHT_CD) AS RELOAN_MCHT_CNT 
#                       FROM THBL.TEMP_RISK_STAT_LAST 
#                       where reloan > 1 
#                       GROUP BY DATA_DT, PROV_CD')
# data.11.4 <- sqlQuery(channel, 'SELECT DATA_DT, PROV_CD,
#                       COUNT(DISTINCT MCHT_CD) AS FINISHED_MCHT
#                       FROM THBL.TEMP_RISK_STAT_LAST 
#                       where overdue_status_3 = 2  
#                       GROUP BY DATA_DT, PROV_CD')


risk.data.all$DATA_DT <- as.Date(risk.data.all$DATA_DT)

odbcClose(channel)

prov <- subset(coding, COL == "PROV_CD")
city <- subset(coding, COL == "SHENGSHI_CD")

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
  paste(c("月汇总"), sep = "")


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
  paste(c("月汇总"), sep = "")


# 续贷逾期情况 Output: table.2; table.2.1
# reloan == 1
data.table.2 <- arrange(risk.data.all[c("DATA_DT", "RELOAN", "OVERDUE_STATUS_3", "CNT", "BAL", "OD_AMT", "MATURITY_DAYS")], DATA_DT)

table.2.part.1.firstLoan <- aggregate(CNT~DATA_DT, subset(data.table.2, RELOAN == 1 & OVERDUE_STATUS_3 != 2), FUN = "sum")

table.2.part.2.firstLoan <- aggregate(CNT~DATA_DT, subset(data.table.2, RELOAN == 1 & OVERDUE_STATUS_3 == 1), FUN = "sum")

table.2.part.3.firstLoan <- aggregate(BAL~DATA_DT, subset(data.table.2, RELOAN == 1), FUN = "sum")

table.2.part.4.firstLoan <- aggregate(OD_AMT~DATA_DT, subset(data.table.2, RELOAN == 1), FUN = "sum")

table.2.firstLoan <- Reduce(function(x,y) merge(x,y, by = "DATA_DT", all = T), list(table.2.part.1.firstLoan, table.2.part.2.firstLoan,
                                                                                    table.2.part.3.firstLoan, table.2.part.4.firstLoan))
table.2.firstLoan$Ratio <-  percent(table.2.firstLoan$OD_AMT/table.2.firstLoan$BAL, d= 2)

# reloan > 1
table.2.part.1.reLoan <- aggregate(CNT~DATA_DT, subset(data.table.2, RELOAN != 1 & OVERDUE_STATUS_3 != 2), FUN = "sum")

table.2.part.2.reLoan <- aggregate(CNT~DATA_DT, subset(data.table.2, RELOAN != 1 & OVERDUE_STATUS_3 == 1), FUN = "sum")

table.2.part.3.reLoan <- aggregate(BAL~DATA_DT, subset(data.table.2, RELOAN != 1), FUN = "sum")

table.2.part.4.reLoan <- aggregate(OD_AMT~DATA_DT, subset(data.table.2, RELOAN != 1), FUN = "sum")

table.2.reLoan <- Reduce(function(x,y) merge(x,y, by = "DATA_DT", all = T), list(table.2.part.1.reLoan, table.2.part.2.reLoan,
                                                                                 table.2.part.3.reLoan, table.2.part.4.reLoan))
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

# reloan == 1
data.table.2.1 <- subset(risk.data.all[c("DATA_DT", "PROV_CD", "RELOAN", "OVERDUE_STATUS_3", "CNT", "BAL", "OD_AMT", "MATURITY_DAYS")], DATA_DT == as.Date("2017-03-30"))

table.2.1.firstLoan <- aggregate(CNT~PROV_CD, subset(data.table.2.1, RELOAN == 1 & OVERDUE_STATUS_3 != 2), FUN = "sum")

table.2.2.firstLoan <- aggregate(CNT~PROV_CD, subset(data.table.2.1, RELOAN == 1 & OVERDUE_STATUS_3 == 1), FUN = "sum")

table.2.3.firstLoan <- aggregate(BAL~PROV_CD, subset(data.table.2.1, RELOAN == 1), FUN = "sum")

table.2.4.firstLoan <- aggregate(OD_AMT~PROV_CD, subset(data.table.2.1, RELOAN == 1), FUN = "sum")

table.2.1.firstLoan <- Reduce(function(x,y) merge(x,y, by = "PROV_CD", all = T), list(table.2.1.firstLoan, table.2.2.firstLoan,
                                                                                      table.2.3.firstLoan, table.2.4.firstLoan))
table.2.1.firstLoan[is.na(table.2.1.firstLoan)] <- 0

table.2.1.firstLoan$Ratio <-  percent(table.2.1.firstLoan$OD_AMT/table.2.1.firstLoan$BAL, d= 2)

# reloan > 1
table.2.1.reLoan <- aggregate(CNT~PROV_CD, subset(data.table.2.1, RELOAN != 1 & OVERDUE_STATUS_3 != 2), FUN = "sum")

table.2.2.reLoan <- aggregate(CNT~PROV_CD, subset(data.table.2.1, RELOAN != 1 & OVERDUE_STATUS_3 == 1), FUN = "sum")

table.2.3.reLoan <- aggregate(BAL~PROV_CD, subset(data.table.2.1, RELOAN != 1), FUN = "sum")

table.2.4.reLoan <- aggregate(OD_AMT~PROV_CD, subset(data.table.2.1, RELOAN != 1), FUN = "sum")

table.2.1.reLoan <- Reduce(function(x,y) merge(x,y, by = "PROV_CD", all = T), list(table.2.1.reLoan, table.2.2.reLoan,
                                                                                   table.2.3.reLoan, table.2.4.reLoan))
table.2.1.reLoan[is.na(table.2.1.reLoan)] <- 0

table.2.1.reLoan$Ratio <- percent(table.2.1.reLoan$OD_AMT/table.2.1.reLoan$BAL, d = 2)

# all
table.2.1.allLoan <- setNames(aggregate(CNT~PROV_CD, subset(data.table.2.1, OVERDUE_STATUS_3 != 2), FUN = "sum"), c("PROV_CD", "CNT.1"))

table.2.2.allLoan <- setNames(aggregate(CNT~PROV_CD, subset(data.table.2.1, OVERDUE_STATUS_3 == 1), FUN = "sum"), c("PROV_CD", "CNT.2"))

table.2.3.allLoan <- setNames(aggregate(BAL~PROV_CD, data.table.2.1, FUN = "sum"), c("PROV_CD", "BAL.1"))

table.2.4.allLoan <- setNames(aggregate(OD_AMT~PROV_CD, data.table.2.1, FUN = "sum"), c("PROV_CD", "OD_AMT.1"))

table.2.5.allLoan <- setNames(aggregate(CNT~PROV_CD, subset(data.table.2.1, MATURITY_DAYS > 0 & OVERDUE_STATUS_3 != 2), FUN = "sum"), c("PROV_CD", "CNT.3"))

table.2.6.allLoan <- setNames(aggregate(OD_AMT~PROV_CD, subset(data.table.2.1, MATURITY_DAYS > 0), FUN = "sum"), c("PROV_CD", "OD_AMT.2"))

table.2.7.allLoan <- setNames(aggregate(CNT~PROV_CD, subset(data.table.2.1, MATURITY_DAYS == 3 & OVERDUE_STATUS_3 != 2), FUN = "sum"), c("PROV_CD", "CNT.4"))

table.2.8.allLoan <- setNames(aggregate(OD_AMT~PROV_CD, subset(data.table.2.1, MATURITY_DAYS == 3), FUN = "sum"), c("PROV_CD", "OD_AMT.3"))

table.2.1.allLoan <- Reduce(function(x,y) merge(x,y, by = "PROV_CD", all = T), list(table.2.1.allLoan, table.2.2.allLoan,
                                                                                    table.2.3.allLoan, table.2.4.allLoan,
                                                                                    table.2.5.allLoan, table.2.6.allLoan,
                                                                                    table.2.7.allLoan, table.2.8.allLoan))
table.2.1.allLoan[is.na(table.2.1.allLoan)] <- 0

table.2.1 <- Reduce(function(x,y) merge(x,y, by = "PROV_CD", all = T), list(table.2.1.firstLoan, table.2.1.reLoan, table.2.1.allLoan))
table.2.1[is.na(table.2.1)] <- 0

table.2.1 <- merge(prov[,-1], table.2.1, by.x = "CODE", by.y = "PROV_CD", all.y = T)

table.2.1.total <- sapply(table.2.1[,c(3:6, 8:11, 13:20)], sum)

table.2.1 <- rbind(table.2.1, c(NA, NA, table.2.1.total[1:4], NA, table.2.1.total[5:8], NA, table.2.1.total[9:16]))
table.2.1[nrow(table.2.1), 7] <- percent(table.2.1[nrow(table.2.1), 6]/table.2.1[nrow(table.2.1), 5], d = 2)
table.2.1[nrow(table.2.1), 12] <- percent(table.2.1[nrow(table.2.1), 11]/table.2.1[nrow(table.2.1), 10], d = 2)

table.2.1 <- data.frame(append(table.2.1, list(ODRATE = percent(table.2.1$OD_AMT.1/table.2.1$BAL.1, d = 2)), after = 16))
table.2.1 <- data.frame(append(table.2.1, list(BORATE = percent(table.2.1$OD_AMT.2/table.2.1$BAL.1, d = 2)), after = 19))
table.2.1 <- data.frame(append(table.2.1, list(BNRATE = percent(table.2.1$OD_AMT.3/table.2.1$BAL.1, d = 2)), after = 22))

names(table.2.1) <- c("公司ID", "CORP", "首贷数量", "首贷逾期数", "首贷余额", "首贷逾期金额", "首贷逾期率", "续贷数量", "续贷逾期数",
                      "续贷余额", "续贷逾期金额", "续贷逾期率", "未结清数", "保理逾期户数", "省余额", "保理逾期金额", "保理逾期率",
                      "银行逾期户数", "银行逾期金额", "银行逾期率", "银行不良户数", "银行不良金额", "银行不良率")

# 还款情况 Output: table.3
data.table.3 <- arrange(risk.data.all[c("DATA_DT", "DIFF_SP_AMT", "DIFF_AP_AMT", "OVERDUE_STATUS_3", "OVERDUE_STATUS_3_LAST")], DATA_DT)

table.3.part.1 <- aggregate(DIFF_SP_AMT~DATA_DT, data.table.3, FUN = "sum")
table.3.part.2 <- aggregate(DIFF_AP_AMT~DATA_DT, data.table.3, FUN = "sum")
table.3.part.3 <- aggregate(DIFF_SP_AMT~DATA_DT, subset(data.table.3, OVERDUE_STATUS_3 == 1 | OVERDUE_STATUS_3_LAST ==1), FUN = "sum")
table.3.part.4 <- aggregate(DIFF_AP_AMT~DATA_DT, subset(data.table.3, OVERDUE_STATUS_3 == 1 | OVERDUE_STATUS_3_LAST == 1), FUN = "sum")

table.3 <- Reduce(function(x,y) merge(x,y, by = "DATA_DT", all = T), list(table.3.part.1, table.3.part.2,
                                                                          table.3.part.3, table.3.part.4))

table.3$title <- paste(month(table.3$DATA_DT), "月回款", sep = "")
table.3 <- table.3[c(ncol(table.3), 1:(ncol(table.3)-1))]

table.3$DATA_DT <- paste(month(as.Date(table.3$DATA_DT) %m-% months(1)), month(table.3$DATA_DT), sep = "-") %>%
  paste(c("月汇总"), sep = "")


# 迁徙率月度情况 Output: table.4.1; table.4.2
data.table.4 <- arrange(risk.data.all[c("DATA_DT", "OVERDUE_STATUS_5", "OVERDUE_STATUS_5_LAST" ,"CNT")], DATA_DT)

table.4.1 <- arrange(aggregate(CNT~DATA_DT+OVERDUE_STATUS_5, data.table.4, FUN = "sum") %>%
                       reshape(idvar = "DATA_DT", timevar = "OVERDUE_STATUS_5", direction = "wide"), DATA_DT)
table.4.1$regular <- table.4.1$CNT.0 + table.4.1$CNT.1


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
table.4.1[is.na(table.4.1)] <- 0
table.4.2[is.na(table.4.2)] <- 0

table.4.2$rate.1 <- percent(table.4.2$`0-1`/table.4.1$regular[-nrow(table.4.1)], d = 2)
table.4.2$rate.2 <- percent(table.4.2$`2-3`/table.4.1$CNT.2[-nrow(table.4.1)], d = 2)
table.4.2$rate.3 <- percent(table.4.2$`2-4`/table.4.1$CNT.2[-nrow(table.4.1)], d = 2)
table.4.2$rate.4 <- percent(table.4.2$`3-4`/table.4.1$CNT.3[-nrow(table.4.1)], d = 2)
table.4.2$rate.5 <- percent(table.4.2$`4-4`/table.4.1$CNT.4[-nrow(table.4.1)], d = 2)

table.4.1$DATA_DT <- substr(table.4.1$DATA_DT, 1, 7)
table.4.2$DATA_DT <- paste(month(as.Date(table.4.2$DATA_DT) %m-% months(1)), month(table.4.2$DATA_DT), sep = "-") %>%
  paste(c("月汇总"), sep = "")

# 资产情况月度分析  Output: table.5.1; table.5.2
data.table.5 <- arrange(risk.data.all[c("DATA_DT", "BEGIN_DATE", "OVERDUE_STATUS_3", "OD_AMT", "LOAN_M_CNT", "NEW_AMT")], BEGIN_DATE)
data.table.5$LOAN_M_CNT <- substr(data.table.5$LOAN_M_CNT, 1, 7)

table.5.1.1 <- arrange(aggregate(NEW_AMT~BEGIN_DATE, data.table.5, FUN = "sum"), BEGIN_DATE)


table.5.1.2 <- arrange(aggregate(OD_AMT~BEGIN_DATE+LOAN_M_CNT,
                                 subset(data.table.5, BEGIN_DATE >= min(data.table.5$DATA_DT)), FUN = "sum"), BEGIN_DATE, as.numeric(LOAN_M_CNT)) %>%
  reshape(idvar = "BEGIN_DATE", timevar = "LOAN_M_CNT", direction = "wide")

table.5.1 <- merge(table.5.1.1, table.5.1.2, all.y = T)


table.5.2 <- cbind(table.5.1[,1:2], table.5.1[,-1:-2]/table.5.1$NEW_AMT)
table.5.2[,-1:-2] <- apply(table.5.2[,-1:-2], c(1,2), function(x) if(!(is.na(x))) percent(x, d = 2) else NA)

table.5.1$BEGIN_DATE <- substr(table.5.1$BEGIN_DATE, 1, 7)
table.5.2$BEGIN_DATE <- substr(table.5.2$BEGIN_DATE, 1, 7)
name.table.5 <- as.numeric(substr(colnames(table.5.1)[-1:-3], 8, nchar(colnames(table.5.1)[-1:-3]))) + 1
name.table.5 <- Reduce(function(x,y) paste(x,y, sep = ""), list("第", name.table.5, "个月"))
colnames(table.5.1) <- c("新增放款月份", "新增放款金额", "当月", name.table.5)
colnames(table.5.2) <- colnames(table.5.1)

# 终止和到期情况 Output: table.6.all; table.6.ex
index.table.6 <- function(x){
  data.table.6 <- arrange(x[c("DATA_DT", "OVERDUE_STATUS_3", "CNT", "OD_AMT", "MATURITY_DAYS", "STATUS_THIS_MONTH")], DATA_DT)
  
  table.6.part.1 <- arrange(aggregate(CNT~DATA_DT, subset(data.table.6, OVERDUE_STATUS_3 == 1), FUN = "sum"), DATA_DT)
  table.6.part.2 <- arrange(aggregate(OD_AMT~DATA_DT, subset(data.table.6, OVERDUE_STATUS_3 == 1), FUN = "sum"), DATA_DT)
  table.6.part.3 <- arrange(aggregate(CNT~DATA_DT, subset(data.table.6, MATURITY_DAYS != -1 & OVERDUE_STATUS_3 != 2), FUN = "sum"), DATA_DT)
  table.6.part.4 <- arrange(aggregate(OD_AMT~DATA_DT, subset(data.table.6, MATURITY_DAYS != -1 & OVERDUE_STATUS_3 != 2), FUN = "sum"), DATA_DT)
  table.6.part.5 <- arrange(aggregate(CNT~DATA_DT, subset(data.table.6, STATUS_THIS_MONTH == 1), FUN = "sum"), DATA_DT)
  table.6.part.6 <- arrange(aggregate(OD_AMT~DATA_DT, subset(data.table.6, STATUS_THIS_MONTH == 1), FUN = "sum"), DATA_DT)
  table.6 <- Reduce(function(x,y) merge(x,y, by = "DATA_DT", all = T), list(table.6.part.1, table.6.part.2, table.6.part.3,
                                                                            table.6.part.4, table.6.part.5, table.6.part.6))
  table.6$DATA_DT <- substr(table.6$DATA_DT, 1, 7)
  return(table.6)
}
table.6.all <- index.table.6(risk.data.all)


# 资产结构类数据 Output: table.7.1; table.7.2; table.7.3
table.7.part.1 <- table.4.1
table.7.part.1$sum <- apply(table.7.part.1[,2:6], 1, sum)
table.7.part.1$type0 <- percent(table.7.part.1$regular/table.7.part.1$sum, d = 2)
table.7.part.1$type1 <- percent(table.7.part.1$CNT.2/table.7.part.1$sum, d = 2)
table.7.part.1$type2 <- percent(table.7.part.1$CNT.3/table.7.part.1$sum, d = 2)
table.7.part.1$type3 <- percent(table.7.part.1$CNT.4/table.7.part.1$sum, d = 2)
table.7.1 <- table.7.part.1[,-2:-8]

data.table.7 <- risk.data.all[c("DATA_DT", "OD_AMT", "OVERDUE_STATUS_5")]

table.7.part.2 <- arrange(aggregate(OD_AMT~DATA_DT+OVERDUE_STATUS_5, subset(data.table.7, OVERDUE_STATUS_5 != 0 & OVERDUE_STATUS_5 != 1), FUN = "sum"), DATA_DT, OVERDUE_STATUS_5) %>%
  reshape(idvar = "DATA_DT", timevar = "OVERDUE_STATUS_5", direction = "wide")
table.7.part.2[is.na(table.7.part.2)] <- 0
table.7.part.2$sum <- apply(table.7.part.2[,2:4], 1, sum)
table.7.part.2$type0 <- percent(table.7.part.2$OD_AMT.2/table.7.part.2$sum, d = 2)
table.7.part.2$type1 <- percent(table.7.part.2$OD_AMT.3/table.7.part.2$sum, d = 2)
table.7.part.2$type2 <- percent(table.7.part.2$OD_AMT.4/table.7.part.2$sum, d = 2)
table.7.2 <- table.7.part.2[, -2:-5]
table.7.2$DATA_DT <- substr(table.7.2$DATA_DT, 1, 7)

data.table.7.3 <- arrange(risk.data.all[c("DATA_DT", "OVERDUE_STATUS_3", "OVERDUE_STATUS_5", "CNT", "OD_PRINCIPAL", "OD_AMT", "BAL")] %>%
                            subset(OVERDUE_STATUS_3 != 2 & as.Date(DATA_DT) >= as.Date('2017-01-01')), DATA_DT)

combined.1 <- subset(data.table.7.3, OVERDUE_STATUS_5 == 0 | OVERDUE_STATUS_5 == 1)
combined.1 <- aggregate(combined.1[,4:7], by = combined.1["DATA_DT"], sum)
combined.1 <- as.data.frame(append(combined.1, list(OVERDUE_STATUS_5 = 0), after = 1))

combined.2 <- subset(data.table.7.3, OVERDUE_STATUS_5 != 0 & OVERDUE_STATUS_5 != 1)
combined.2 <- aggregate(combined.2[,4:7], by = list(DATA_DT = combined.2$DATA_DT, OVERDUE_STATUS_5 = combined.2$OVERDUE_STATUS_5), sum)

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
table.7.part.3.4 <- arrange(aggregate(OD_PRINCIPAL~DATA_DT+OVERDUE_STATUS_5, data.table.7.3, FUN = "sum"), DATA_DT)
table.7.part.3.5 <- arrange(aggregate(OD_AMT~DATA_DT+OVERDUE_STATUS_5, data.table.7.3, FUN = "sum"), DATA_DT)
table.7.part.3.6 <- arrange(aggregate(BAL~DATA_DT+OVERDUE_STATUS_5, data.table.7.3, FUN = "sum"), DATA_DT)
table.7.part.3.6[is.na(table.7.part.3.5)] <- 0
table.7.part.3.7 <- arrange(aggregate(Ratio.AMT~DATA_DT+OVERDUE_STATUS_5, data.table.7.3, FUN = "sum"), DATA_DT)

table.7.3 <- Reduce(function(x,y) merge(x,y, all = T), list(table.7.part.3.1, table.7.part.3.2, table.7.part.3.4,
                                                            table.7.part.3.5, table.7.part.3.6, table.7.part.3.7))

summary.7.3 <- aggregate(table.7.3[,-1:-2], by = table.7.3["DATA_DT"], FUN = "sum")
table.7.3$Ratio.CNT <- percent(table.7.3$Ratio.CNT, d = 2)
table.7.3$Ratio.AMT <- percent(table.7.3$Ratio.AMT, d = 2)
table.7.3$OVERDUE_STATUS_5 <- lapply(table.7.3$OVERDUE_STATUS_5, function(x) case_when(x == 0 ~ "正常",
                                                                                       x == 2 ~ "一般",
                                                                                       x == 3 ~ "催收",
                                                                                       x == 4 ~ "严重"))
diff.7.3 <- summary.7.3[-1,] - summary.7.3[-nrow(summary.7.3),]
diff.7.3$DATA_DT <- summary.7.3$DATA_DT[-1]
diff.7.3$Ratio.CNT <- NA
diff.7.3$Ratio.AMT <- NA
diff.7.3$OVERDUE_STATUS_5 <- c("增量")
summary.7.3$OVERDUE_STATUS_5 <- c("汇总")
summary.7.3$Ratio.CNT <- percent(summary.7.3$Ratio.CNT, d = 2)
summary.7.3$Ratio.AMT <- percent(summary.7.3$Ratio.AMT, d = 2)
sum.diff.7.3 <- rbind(summary.7.3, diff.7.3)
sum.diff.7.3 <- sum.diff.7.3[,c(1,8,2:7)]
table.7.3 <- arrange(rbind(table.7.3, sum.diff.7.3),DATA_DT)
table.7.3$DATA_DT <- substr(table.7.3$DATA_DT, 1, 7)
colnames(table.7.1) <- c("month", "正常", "一般", "催收", "严重")
colnames(table.7.2) <- c("month", "一般", "催收", "严重")
colnames(table.7.3) <- c("month", "状态", "未结清户数", "占比", "逾期本金", "逾期总额", "余额", "占比")

# 放款情况 OUTPUT: table.8.1; table.8.2; table.8.3
data.table.8 <- risk.data.all[c("DATA_DT", "PROV_CD", "SHENGSHI_CD", "OVERDUE_STATUS_3", "CNT", "LOAN_PR", "BAL")]

# data.table.8.1 <- subset(data.table.8, DATA_DT == as.Date("2017-03-30")) # test for accuracy
data.table.8.1 <- subset(data.table.8, DATA_DT == max(data.table.8$DATA_DT))
table.8.2.1 <- aggregate(data.table.8.1[,-1:-4], by = list(SHENGSHI_CD = data.table.8.1$SHENGSHI_CD), FUN = "sum")
table.8.2.2 <- aggregate(CNT~SHENGSHI_CD, subset(data.table.8.1, OVERDUE_STATUS_3 != 2), FUN = "sum")
table.8.2 <- merge(table.8.2.1, table.8.2.2, by = "SHENGSHI_CD", all =T)
table.8.2 <- merge(table.8.2, city[,-1], by.x = "SHENGSHI_CD", by.y = "CODE", all.x = T)
table.8.2 <- table.8.2[,c(6,1:3,5,4)]
table.8.2[is.na(table.8.2)] <- 0
table.8.2 <- rbind(table.8.2, c(NA, NA, sapply(table.8.2[,3:6], sum)))

table.8.1.1 <- aggregate(data.table.8.1[,-1:-4], by = list(PROV_CD = data.table.8.1$PROV_CD), FUN = "sum")
table.8.1.2 <- aggregate(CNT~PROV_CD, subset(data.table.8.1, OVERDUE_STATUS_3 != 2), FUN = "sum")
table.8.1 <- merge(table.8.1.1, table.8.1.2, by = "PROV_CD", all = T)
table.8.1 <- merge(table.8.1, prov[,-1], by.x = "PROV_CD", by.y = "CODE", all.x = T)
table.8.1 <- table.8.1[,c(1,6,2:3,5,4)]
table.8.1[is.na(table.8.1)] <- 0
table.8.1 <- rbind(table.8.1, c(NA, NA, sapply(table.8.1[,3:6], sum)))

data.table.8.3 <- risk.data.all[c("DATA_DT", "NEW_LOAN", "CNT", "NEW_AMT", "LOAN_PR")]
table.8.3.1 <- aggregate(subset(data.table.8.3, NEW_LOAN == 1)[,3:4], by = list(DATA_DT = subset(data.table.8.3, NEW_LOAN == 1)$DATA_DT), "sum")
table.8.3.2 <- aggregate(data.table.8.3[,c(5,3)], by = list(DATA_DT = data.table.8.3$DATA_DT), "sum")
table.8.3 <- merge(table.8.3.1, table.8.3.2, by = "DATA_DT", all = T)
table.8.3$DATA_DT <- substr(table.8.3$DATA_DT, 1, 7)
names(table.8.3) <- c("月份", "新增放款户数", "新增放款金额", "当月累计金额", "当月累计户数")

# 逾期率及不良率 OUTPUT: table.9.1; table.9.2; table.9.3; table.9.4
data.table.9 <- risk.data.all[c("DATA_DT", "OVERDUE_STATUS_3", "CNT", "OD_AMT", "LOAN_PR", "MATURITY_DAYS", "BAL", "PROV_CD", "SHENGSHI_CD", "LOAN_PR_SCOPE")]

table.9.1.1 <- setNames(arrange(aggregate(CNT~DATA_DT, subset(data.table.9, OVERDUE_STATUS_3 != 2), "sum"), DATA_DT), c("DATA_DT", "CNT.1"))
table.9.1.2 <- arrange(aggregate(data.table.9[c("CNT", "LOAN_PR", "OD_AMT", "BAL")], by = list(DATA_DT = data.table.9$DATA_DT), "sum"), DATA_DT)
data.table.9.1 <- subset(data.table.9, OVERDUE_STATUS_3 == 1)
table.9.1.3 <- setNames(arrange(aggregate(data.table.9.1[c("CNT")], list(DATA_DT = data.table.9.1$DATA_DT), "sum"), DATA_DT), c("DATA_DT", "CNT.2"))
data.table.9.1 <- subset(data.table.9, OVERDUE_STATUS_3 == 1 & MATURITY_DAYS > 0)
table.9.1.4 <- setNames(arrange(aggregate(data.table.9.1[c("CNT", "OD_AMT")], list(DATA_DT = data.table.9.1$DATA_DT), "sum"), DATA_DT), c("DATA_DT", "CNT.X", "OD_AMT.X"))
data.table.9.1 <- subset(data.table.9, OVERDUE_STATUS_3 == 1 & MATURITY_DAYS == 3)
table.9.1.5 <- setNames(arrange(aggregate(data.table.9.1[c("CNT", "OD_AMT")], list(DATA_DT = data.table.9.1$DATA_DT), "sum"), DATA_DT), c("DATA_DT", "CNT.Y", "OD_AMT.Y"))
table.9.1 <- Reduce(function(x,y) merge(x,y, by = "DATA_DT", all = T), list(table.9.1.1, table.9.1.2, table.9.1.3, table.9.1.4, table.9.1.5))
table.9.1$DATA_DT <- substr(table.9.1$DATA_DT, 1, 7)
table.9.1[is.na(table.9.1)] <- 0
table.9.1 <- table.9.1[c(1:4, 7:8, 10, 5, 9, 11, 6)]
table.9.1$rate.1 <- percent(table.9.1$OD_AMT/table.9.1$BAL, d = 2)
table.9.1$rate.2 <- percent(table.9.1$OD_AMT.X/table.9.1$BAL, d = 2)
table.9.1$rate.3 <- percent(table.9.1$OD_AMT.Y/table.9.1$BAL, d = 2)

# data.table.9.2 <- subset(data.table.9, DATA_DT == as.Date("2017-03-30")) # test for accuracy
data.table.9.2 <- subset(data.table.9, DATA_DT == max(data.table.9$DATA_DT))
table.9.2.6 <- setNames(arrange(aggregate(BAL~PROV_CD, subset(data.table.9.2, OVERDUE_STATUS_3 == 1), "sum"), PROV_CD),c("PROV_CD", "BAL.od"))
table.9.2.1 <- setNames(arrange(aggregate(CNT~PROV_CD, subset(data.table.9.2, OVERDUE_STATUS_3 != 2), "sum"), PROV_CD), c("PROV_CD", "CNT.1"))
table.9.2.2 <- arrange(aggregate(data.table.9.2[c("BAL")], by = list(PROV_CD = data.table.9.2$PROV_CD), "sum"), PROV_CD)
data.table.9.2.1 <- subset(data.table.9.2, OVERDUE_STATUS_3 == 1)
table.9.2.3 <- setNames(arrange(aggregate(data.table.9.2.1[c("CNT", "OD_AMT")], list(PROV_CD = data.table.9.2.1$PROV_CD), "sum"), PROV_CD), c("PROV_CD", "CNT.2", "OD_AMT"))
data.table.9.2.2 <- subset(data.table.9.2, OVERDUE_STATUS_3 == 1 & MATURITY_DAYS > 0)
table.9.2.4 <- setNames(arrange(aggregate(data.table.9.2.2[c("CNT", "OD_AMT")], list(PROV_CD = data.table.9.2.2$PROV_CD), "sum"), PROV_CD), c("PROV_CD", "CNT.X", "OD_AMT.X"))
data.table.9.2.3 <- subset(data.table.9.2, OVERDUE_STATUS_3 == 1 & MATURITY_DAYS == 3)
table.9.2.5 <- setNames(arrange(aggregate(data.table.9.2.3[c("CNT", "OD_AMT")], list(PROV_CD = data.table.9.2.3$PROV_CD), "sum"), PROV_CD), c("PROV_CD", "CNT.Y", "OD_AMT.Y"))
table.9.2 <- Reduce(function(x,y) merge(x,y, by = "PROV_CD", all = T), list(table.9.2.1, table.9.2.2, table.9.2.3, table.9.2.4, table.9.2.5, table.9.2.6))
table.9.2$PROV_CD <- substr(table.9.2$PROV_CD, 1, 7)
table.9.2[is.na(table.9.2)] <- 0
table.9.2 <- merge(prov[,-1], table.9.2, by.x = "CODE", by.y = "PROV_CD", all.y = T)
table.9.2 <- rbind(table.9.2, c(NA, NA, sapply(table.9.2[,-1:-2], sum)))
table.9.2 <- data.frame(append(table.9.2, list(rate.1 = percent(table.9.2$OD_AMT/table.9.2$BAL, d = 2)), after = 6))
table.9.2 <- data.frame(append(table.9.2, list(rate.2 = percent(table.9.2$OD_AMT.X/table.9.2$BAL, d = 2)), after = 9))
table.9.2 <- data.frame(append(table.9.2, list(rate.3 = percent(table.9.2$OD_AMT.Y/table.9.2$BAL, d = 2)), after = 12))
table.9.2 <- data.frame(append(table.9.2, list(rate.4 = percent(table.9.2$BAL.od/table.9.2$BAL, d = 2)), after = 14))

table.9.3.1 <- setNames(arrange(aggregate(CNT~SHENGSHI_CD, subset(data.table.9.2, OVERDUE_STATUS_3 != 2), "sum"), SHENGSHI_CD), c("SHENGSHI_CD", "CNT.1"))
table.9.3.2 <- arrange(aggregate(BAL~SHENGSHI_CD, data.table.9.2, "sum"), SHENGSHI_CD)
table.9.3.3 <- arrange(aggregate(data.table.9.2.1[c("CNT", "OD_AMT")], by = list(SHENGSHI_CD = data.table.9.2.1$SHENGSHI_CD), "sum"), SHENGSHI_CD)
table.9.3 <- Reduce(function(x,y) merge(x,y, by = "SHENGSHI_CD", all = T), list(table.9.3.1, table.9.3.2, table.9.3.3))
table.9.3 <- merge(city[,-1], table.9.3, by.x = "CODE", by.y = "SHENGSHI_CD", all.y = T)
table.9.3[is.na(table.9.3)] <- 0
table.9.3 <- rbind(table.9.3, c(NA, NA, sapply(table.9.3[,-1:-2], sum)))
table.9.3 <- data.frame(append(table.9.3, list(rate = percent(table.9.3$OD_AMT/table.9.3$BAL, d = 2)), after = ncol(table.9.3)))

data.table.9.4 <- subset(data.table.9.2, OVERDUE_STATUS_3 != 2)
table.9.4.1 <- arrange(aggregate(data.table.9.4[c("CNT", "LOAN_PR", "BAL")], by = list(LOAN_PR_SCOPE = data.table.9.4$LOAN_PR_SCOPE), "sum"), LOAN_PR_SCOPE)
table.9.4.2 <- arrange(aggregate(data.table.9.2.1[c("CNT", "OD_AMT")], by = list(LOAN_PR_SCOPE = data.table.9.2.1$LOAN_PR_SCOPE), "sum"), LOAN_PR_SCOPE)
table.9.4 <- Reduce(function(x,y) merge(x,y, by = "LOAN_PR_SCOPE", all = T), list(table.9.4.1, table.9.4.2))
table.9.4 <- rbind(table.9.4, c(NA, sapply(table.9.4[,-1], sum)))
table.9.4 <- data.frame(append(table.9.4, list(rate = percent(table.9.4$OD_AMT/table.9.4$BAL, d = 2)), after = ncol(table.9.4)))

names(table.9.2) <- c("CORP", "公司ID", "未结清户数", "省余额", "保理逾期户数", "保理逾期金额", "保理逾期率", "银行逾期户数",
                      "银行逾期金额", "银行逾期率", "银行不良户数", "银行不良金额", "银行不良率", "逾期余额", "余额逾期率")
names(table.9.3) <- c("编码", "地市营业部", "未结清数", "余额", "逾期数", "逾期金额", "逾期率")

# 黄绿灯逾期（历史值）OUTPUT:table.10.1; table.10.2
data.table.10 <- subset(risk.data.all[c("DATA_DT", "OVERDUE_STATUS_3", "LIGHT", "BAL", "CNT", "OD_AMT", "PROV_CD")], OVERDUE_STATUS_3 != 2)

data.table.10.1 <- subset(data.table.10, OVERDUE_STATUS_3 == 1)
table.10.1.1 <- arrange(aggregate(data.table.10[c("CNT", "BAL")], by = list(DATA_DT = data.table.10$DATA_DT), "sum"), DATA_DT)
table.10.1.2 <- setNames(arrange(aggregate(data.table.10.1[c("CNT", "OD_AMT")], by = list(DATA_DT = data.table.10.1$DATA_DT), "sum"), DATA_DT), c("DATA_DT", "CNT_OD", "OD_AMT_OD"))
table.10.1.3 <- arrange(aggregate(data.table.10[c("CNT", "BAL")], by = list(DATA_DT = data.table.10$DATA_DT, LIGHT = data.table.10$LIGHT), "sum"), DATA_DT, LIGHT) %>%
  reshape(idvar = "DATA_DT", timevar = "LIGHT", direction = "wide")
table.10.1.4 <- arrange(aggregate(data.table.10.1[c("CNT", "OD_AMT")], by = list(DATA_DT = data.table.10.1$DATA_DT, LIGHT = data.table.10.1$LIGHT), "sum"), DATA_DT, LIGHT) %>%
  reshape(idvar = "DATA_DT", timevar = "LIGHT", direction = "wide")
table.10.1 <- Reduce(function(x,y) merge(x,y, by = "DATA_DT", all = T), list(table.10.1.1, table.10.1.2, table.10.1.3, table.10.1.4))
table.10.1[is.na(table.10.1)] <- 0
table.10.1[,6:13] <- table.10.1[,c("CNT.1.x", "BAL.1", "CNT.1.y", "OD_AMT.1", "CNT.0.x", "BAL.0", "CNT.0.y", "OD_AMT.0")]
table.10.1 <- data.frame(append(table.10.1, list(rate.1 = percent(table.10.1$OD_AMT_OD/table.10.1$BAL, d = 2)), after = 5))
table.10.1 <- data.frame(append(table.10.1, list(rate.2 = percent(table.10.1[,10]/table.10.1[,8], d = 2)), after = 10))
table.10.1 <- data.frame(append(table.10.1, list(rate.3 = percent(table.10.1[,15]/table.10.1[,13], d = 2)), after = ncol(table.10.1)))
table.10.1$DATA_DT <- substr(table.10.1$DATA_DT, 1, 7)

# data.table.10.2 <- subset(data.table.10, DATA_DT == as.Date("2017-03-30")) # test for accuracy
data.table.10.2 <- subset(data.table.10, DATA_DT == max(data.table.10$DATA_DT))
data.table.10.2.1 <- subset(data.table.10.2, OVERDUE_STATUS_3 == 1)

table.10.2.1 <- arrange(aggregate(data.table.10.2[c("CNT", "BAL")], by = list(PROV_CD = data.table.10.2$PROV_CD), "sum"), PROV_CD)
table.10.2.2 <- setNames(arrange(aggregate(data.table.10.2.1[c("CNT", "OD_AMT")], by = list(PROV_CD = data.table.10.2.1$PROV_CD), "sum"), PROV_CD), c("PROV_CD", "CNT_OD", "OD_AMT_OD"))
table.10.2.3 <- arrange(aggregate(data.table.10.2[c("CNT", "BAL")], by = list(PROV_CD = data.table.10.2$PROV_CD, LIGHT = data.table.10.2$LIGHT), "sum"), PROV_CD, LIGHT) %>%
  reshape(idvar = "PROV_CD", timevar = "LIGHT", direction = "wide")
table.10.2.4 <- arrange(aggregate(data.table.10.2.1[c("CNT", "OD_AMT")], by = list(PROV_CD = data.table.10.2.1$PROV_CD, LIGHT = data.table.10.2.1$LIGHT), "sum"), PROV_CD, LIGHT) %>%
  reshape(idvar = "PROV_CD", timevar = "LIGHT", direction = "wide")
table.10.2 <- Reduce(function(x,y) merge(x,y, by = "PROV_CD", all = T), list(table.10.2.1, table.10.2.2, table.10.2.3, table.10.2.4))
table.10.2[is.na(table.10.2)] <- 0
table.10.2[,6:13] <- table.10.2[,c("CNT.1.x", "BAL.1", "CNT.1.y", "OD_AMT.1", "CNT.0.x", "BAL.0", "CNT.0.y", "OD_AMT.0")]
table.10.2 <- data.frame(append(table.10.2, list(rate.1 = percent(table.10.2$OD_AMT_OD/table.10.2$BAL, d = 2)), after = 5))
table.10.2 <- data.frame(append(table.10.2, list(rate.2 = percent(table.10.2[,10]/table.10.2[,8], d = 2)), after = 10))
table.10.2 <- data.frame(append(table.10.2, list(rate.3 = percent(table.10.2[,15]/table.10.2[,13], d = 2)), after = ncol(table.10.2)))

table.10.2 <- merge(prov[,-1], table.10.2, by.x = "CODE", by.y = "PROV_CD", all.y = T)
names(table.10.2) <- c("编号", "分公司", "未结清数", "余额", "逾期数", "逾期总额", "逾期率", "未结清_黄灯", "黄灯余额", "黄灯逾期数", "黄灯逾期金额", "黄灯逾期率",
                       "未结清_绿灯", "绿灯余额", "绿灯逾期数", "绿灯逾期金额", "绿灯逾期率")

# 续贷情况（历史值）OUTPUT: table.11.1; table.11.2; table.11.3
# 
# table.11.2 <- merge(data.11.3, data.11.4, by = c("DATA_DT", "PROV_CD"), all = T)
# table.11.2 <- merge(prov[,-1], table.11.2, by.x = "CODE", by.y = "PROV_CD", all.y = T)
# table.11.2 <- arrange(table.11.2, CODE, DATA_DT)
# table.11.2[is.na(table.11.2)] <- 0
# table.11.2$rate <- percent(table.11.2$RELOAN_MCHT_CNT/table.11.2$FINISHED_MCHT, d = 2)
# table.11.2$DATA_DT <- substr(table.11.2$DATA_DT, 1, 7)

table.11.1.1 <- aggregate(CNT~DATA_DT, subset(risk.data.all, RELOANTIMES == 2), "sum")
table.11.1.2 <- aggregate(CNT~DATA_DT, subset(risk.data.all, RELOANTIMES == 1 & OVERDUE_STATUS_3 == 2), "sum")
table.11.1 <- arrange(merge(table.11.1.1, table.11.1.2, by = "DATA_DT", all = T), DATA_DT)
table.11.1$DATA_DT <- substr(table.11.1$DATA_DT, 1, 7)
table.11.1$rate <- percent(table.11.1$CNT.x/table.11.1$CNT.y, d = 2)

data.table.11.2.1 <- subset(risk.data.all, RELOANTIMES == 2)
table.11.2.1 <- aggregate(data.table.11.2.1$CNT, by = list(PROV_CD = data.table.11.2.1$PROV_CD, DATA_DT = data.table.11.2.1$DATA_DT), "sum")

data.table.11.2.2 <- subset(risk.data.all, RELOANTIMES == 1 & OVERDUE_STATUS_3 == 2)
table.11.2.2 <- aggregate(data.table.11.2.2$CNT, by = list(PROV_CD = data.table.11.2.2$PROV_CD, DATA_DT = data.table.11.2.2$DATA_DT), "sum")

table.11.2 <- merge(table.11.2.1, table.11.2.2, by = c("DATA_DT", "PROV_CD"), all = T)
table.11.2[is.na(table.11.2)] <- 0
table.11.2 <- arrange(merge(prov[,-1], table.11.2, by.x = "CODE", by.y = "PROV_CD", all.y = T), CODE, DATA_DT)
table.11.2$rate <- percent(table.11.2$x.x/table.11.2$x.y, d = 2)
table.11.2$DATA_DT <- substr(table.11.2$DATA_DT, 1, 7)

data.11.3 <- subset(risk.data.all, NEW_LOAN == 1 & RELOAN > 1)
table.11.3.1 <- aggregate(data.11.3[c("CNT", "LOAN_PR")], by = list(BEGIN_DATE = data.11.3$BEGIN_DATE), "sum")
table.11.3.1$BEGIN_DATE <- substr(table.11.3.1$BEGIN_DATE, 1, 7)

data.11.3.1 <- subset(risk.data.all, RELOANTIMES != 1)
table.11.3.2 <- aggregate(data.11.3.1[c("CNT", "LOAN_PR")], by = list(DATA_DT = data.11.3.1$DATA_DT), "sum")
table.11.3.2$DATA_DT <- substr(table.11.3.2$DATA_DT, 1, 7)

table.11.3 <- merge(table.11.3.1, table.11.3.2, by.x = "BEGIN_DATE", by.y = "DATA_DT", all = T)
names(table.11.3) <- c("月份", "续贷人次", "续贷金额", "累计续贷人次", "累计续贷金额")

# OUTPUT
# template
ExcelFile <- "E:\\Allinpay\\Data\\TeamWork\\dataForReport\\templateForBR.xls"
template <- paste0("E:\\Allinpay\\Data\\TeamWork\\dataForReport\\bisRpts_exp_", Sys.Date(), ".xls")
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
                     startRow = 4, startCol = 1)
writeWorksheetToFile(template, data = table.2.1,
                     sheet = "续贷逾期情况", header = T,
                     startRow = 6 + nrow(table.2), startCol = 1)
writeWorksheetToFile(template, data = table.3,
                     sheet = "还款情况", header = F,
                     startRow = 3, startCol = 1)
writeWorksheetToFile(template, data = table.4.1,
                     sheet = "迁徙率月度情况", header = F,
                     startRow = 2, startCol = 1)
writeWorksheetToFile(template, data = table.4.2,
                     sheet = "迁徙率月度情况", header = F,
                     startRow = 2, startCol = ncol(table.4.1) + 1)
writeWorksheetToFile(template, data = table.5.1,
                     sheet = "资产情况月度分析", header = T,
                     startRow = 3, startCol = 1)
writeWorksheetToFile(template, data = table.5.2,
                     sheet = "资产情况月度分析", header = T,
                     startRow = nrow(table.5.1)+4, startCol = 1)
writeWorksheetToFile(template, data = table.6.all,
                     sheet = "终止和到期情况", header = F,
                     startRow = 4, startCol = 1)
writeWorksheetToFile(template, data = table.7.1,
                     sheet = "资产结构类数据", header = T,
                     startRow = 2, startCol = 1)
writeWorksheetToFile(template, data = c("金额比"),
                     sheet = "资产结构类数据", header = F,
                     startRow = nrow(table.7.1) + 3, startCol = 1)
writeWorksheetToFile(template, data = table.7.2,
                     sheet = "资产结构类数据", header = T,
                     startRow = nrow(table.7.1) + 4, startCol = 1)
writeWorksheetToFile(template, data = table.7.3,
                     sheet = "资产结构类数据", header = T,
                     startRow = nrow(table.7.1) + nrow(table.7.1) + 6, startCol = 1)
writeWorksheetToFile(template, data = table.8.1,
                     sheet = "放款情况", header = F,
                     startRow = 2, startCol = 1)
writeWorksheetToFile(template, data = table.8.2,
                     sheet = "放款情况", header = F,
                     startRow = 2, startCol = 11)
writeWorksheetToFile(template, data = table.8.3,
                     sheet = "放款情况", header = T,
                     startRow = 25, startCol = 3)
writeWorksheetToFile(template, data = table.9.1,
                     sheet = "逾期率及不良率", header = F,
                     startRow = 2, startCol = 1)
writeWorksheetToFile(template, data = table.9.2,
                     sheet = "逾期率及不良率", header = T,
                     startRow = 4 + nrow(table.9.1), startCol = 1)
writeWorksheetToFile(template, data = table.9.3,
                     sheet = "逾期率及不良率", header = T,
                     startRow = 6 + nrow(table.9.1) + nrow(table.9.2), startCol = 1)
writeWorksheetToFile(template, data = table.9.4[,-1],
                     sheet = "逾期率及不良率", header = F,
                     startRow = 3, startCol = 19)
writeWorksheetToFile(template, data = table.10.1,
                     sheet = "黄绿灯逾期（历史值）", header = F,
                     startRow = 3, startCol = 4)
writeWorksheetToFile(template, data = table.10.2,
                     sheet = "黄绿灯逾期（历史值）", header = T,
                     startRow = 5 + nrow(table.10.1), startCol = 4)
writeWorksheetToFile(template, data = table.11.1,
                     sheet = "续贷情况（历史值）", header = F,
                     startRow = 3, startCol = 1)
writeWorksheetToFile(template, data = table.11.2,
                     sheet = "续贷情况（历史值）", header = F,
                     startRow = 3, startCol = 11)
writeWorksheetToFile(template, data = c("借据号概念"),
                     sheet = "续贷情况（历史值）", header = F,
                     startRow = 5 + nrow(table.11.1), startCol = 1)
writeWorksheetToFile(template, data = table.11.3,
                     sheet = "续贷情况（历史值）", header = T,
                     startRow = 6 + nrow(table.11.1), startCol = 1)






















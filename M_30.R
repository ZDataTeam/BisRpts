library(RODBC)
library(dplyr)
library(tidyr)
library(lubridate)
library(sca)
library(XLConnect)

channel <- odbcConnect("oracle", uid = "thbl", pwd = "thblserver")

# 15:28 PM 之后跑
# 月报月末序列
month.date <- as.list(seq.Date(from = as.Date('2015/09/01'), to =Sys.Date(), by = "month") - 1) %>%
  lapply(function(x) as.character(x, '%Y%m%d'))

every.last.month.date <- as.list(seq.Date(from = as.Date('2015/09/01'), to =Sys.Date(), by = "month") - 1) %>%
  sapply(function(x) paste('to_date(\'', x, '\',\'YYYY-MM-DD\')', sep = ""))
every.last.month.date <-  Reduce(function(x,y) paste(x, y, sep = ","), every.last.month.date)


# 月
sapply(month.date, function(x) sqlQuery(channel, paste('select wrapper_func(\'begin risk_stat_month(', x, ', 0); end;\') from dual;')))


#月报
risk.data.all <- sqlQuery(channel, paste('select * from thbl.risk_statistics_all where data_dt in (', every.last.month.date,')'))

risk.data.exp <- sqlQuery(channel, paste('select * from thbl.risk_statistics_all where data_dt in (', every.last.month.date,')
                                         and prov_cd not like \'3502\''))

coding <- sqlQuery(channel, 'select * from THBL.RISK_DIMENSION')


risk.data.all$DATA_DT <- as.Date(format(risk.data.all$DATA_DT, '%Y-%m-%d'))
risk.data.exp$DATA_DT <- as.Date(format(risk.data.exp$DATA_DT, '%Y-%m-%d'))

odbcClose(channel)

# prov <- subset(coding, COL == "PROV_CD")
# city <- subset(coding, COL == "SHENGSHI_CD")

overdueIndex <- function(data, type){
  # 续贷逾期情况 Output: table.2; table.2.1
  # reloan == 1
  # data <- arrange(data[c("DATA_DT", "RELOAN", "OVERDUE_STATUS_3_30", "CNT", "interest", "OD_AMT", "MATURITY_DAYS")], DATA_DT)
  
  table.2.part.1.firstLoan <- aggregate(CNT~DATA_DT, subset(data, RELOAN == 1 & OVERDUE_STATUS_3_30 != 2), FUN = "sum")
  
  table.2.part.2.firstLoan <- aggregate(CNT~DATA_DT, subset(data, RELOAN == 1 & OVERDUE_STATUS_3_30 == 1), FUN = "sum")
  
  if(type == "interest"){
    table.2.part.3.firstLoan <- aggregate(LOAN_PR_FEE~DATA_DT, subset(data, RELOAN == 1), FUN = "sum")
    table.2.part.4.firstLoan <- aggregate(OD_AMT_30~DATA_DT, subset(data, RELOAN == 1), FUN = "sum")
    table.2.part.5.firstLoan <- aggregate(OD_AMT_30~DATA_DT, subset(data, RELOAN != 1 & MATURITY_DAYS >= 3), FUN = "sum")
    table.2.firstLoan <- Reduce(function(x,y) merge(x,y, by = "DATA_DT", all = T), list(table.2.part.1.firstLoan, table.2.part.2.firstLoan,
                                                                                        table.2.part.3.firstLoan, table.2.part.4.firstLoan,
                                                                                        table.2.part.5.firstLoan))
    table.2.firstLoan[is.na(table.2.firstLoan)] <- 0
    table.2.firstLoan$Ratio <-  percent(table.2.firstLoan$OD_AMT_30.x/table.2.firstLoan$LOAN_PR_FEE, d= 2)
    table.2.firstLoan$BNRatio <-  percent(table.2.firstLoan$OD_AMT_30.y/table.2.firstLoan$LOAN_PR_FEE, d = 2)
    
  }else if(type == "withoutInterest"){
    table.2.part.3.firstLoan <- aggregate(LOAN_PR~DATA_DT, subset(data, RELOAN == 1), FUN = "sum")
    table.2.part.4.firstLoan <- aggregate(OD_PRINCIPAL_30~DATA_DT, subset(data, RELOAN == 1), FUN = "sum")
    table.2.part.5.firstLoan <- aggregate(OD_PRINCIPAL_30~DATA_DT, subset(data, RELOAN != 1 & MATURITY_DAYS >= 3), FUN = "sum")
    table.2.firstLoan <- Reduce(function(x,y) merge(x,y, by = "DATA_DT", all = T), list(table.2.part.1.firstLoan, table.2.part.2.firstLoan,
                                                                                        table.2.part.3.firstLoan, table.2.part.4.firstLoan,
                                                                                        table.2.part.5.firstLoan))
    table.2.firstLoan[is.na(table.2.firstLoan)] <- 0
    table.2.firstLoan$Ratio <-  percent(table.2.firstLoan$OD_PRINCIPAL_30.x/table.2.firstLoan$LOAN_PR, d= 2)
    table.2.firstLoan$BNRatio <- percent(table.2.firstLoan$OD_PRINCIPAL_30.y/table.2.firstLoan$LOAN_PR, d = 2)
    
  }
  # table.2.part.3.firstLoan <- aggregate(BAL~DATA_DT, subset(data, RELOAN == 1), FUN = "sum")
  
  # table.2.part.4.firstLoan <- aggregate(OD_AMT~DATA_DT, subset(data, RELOAN == 1), FUN = "sum")
  
  # table.2.firstLoan <- Reduce(function(x,y) merge(x,y, by = "DATA_DT", all = T), list(table.2.part.1.firstLoan, table.2.part.2.firstLoan,
  #                                                                                     table.2.part.3.firstLoan, table.2.part.4.firstLoan))
  # table.2.firstLoan$Ratio <-  percent(table.2.firstLoan$OD_AMT/table.2.firstLoan$BAL, d= 2)
  
  # reloan > 1
  table.2.part.1.reLoan <- aggregate(CNT~DATA_DT, subset(data, RELOAN != 1 & OVERDUE_STATUS_3_30 != 2), FUN = "sum")
  
  table.2.part.2.reLoan <- aggregate(CNT~DATA_DT, subset(data, RELOAN != 1 & OVERDUE_STATUS_3_30 == 1), FUN = "sum")
  
  if(type == "interest"){
    table.2.part.3.reLoan <- aggregate(LOAN_PR_FEE~DATA_DT, subset(data, RELOAN != 1), FUN = "sum")
    
    table.2.part.4.reLoan <- aggregate(OD_AMT_30~DATA_DT, subset(data, RELOAN != 1), FUN = "sum")
    
    table.2.part.5.reLoan <- aggregate(OD_AMT_30~DATA_DT, subset(data, RELOAN != 1 & MATURITY_DAYS >= 3), FUN = "sum")
    
    table.2.reLoan <- Reduce(function(x,y) merge(x,y, by = "DATA_DT", all = T), list(table.2.part.1.reLoan, table.2.part.2.reLoan,
                                                                                     table.2.part.3.reLoan, table.2.part.4.reLoan,
                                                                                     table.2.part.5.reLoan))
    table.2.reLoan[is.na(table.2.reLoan)] <- 0
    table.2.reLoan$Ratio <- percent(table.2.reLoan$OD_AMT_30.x/table.2.reLoan$LOAN_PR_FEE, d = 2)
    
    table.2.reLoan$BNRatio <- percent(table.2.reLoan$OD_AMT_30.y/table.2.reLoan$LOAN_PR_FEE, d = 2)
  }else if(type == "withoutInterest"){
    table.2.part.3.reLoan <- aggregate(LOAN_PR~DATA_DT, subset(data, RELOAN != 1), FUN = "sum")
    
    table.2.part.4.reLoan <- aggregate(OD_PRINCIPAL_30~DATA_DT, subset(data, RELOAN != 1), FUN = "sum")
    
    table.2.part.5.reLoan <- aggregate(OD_PRINCIPAL_30~DATA_DT, subset(data, RELOAN != 1 & MATURITY_DAYS >= 3), FUN = "sum")
    
    table.2.reLoan <- Reduce(function(x,y) merge(x,y, by = "DATA_DT", all = T), list(table.2.part.1.reLoan, table.2.part.2.reLoan,
                                                                                     table.2.part.3.reLoan, table.2.part.4.reLoan,
                                                                                     table.2.part.5.reLoan))
    table.2.reLoan[is.na(table.2.reLoan)] <- 0
    table.2.reLoan$Ratio <- percent(table.2.reLoan$OD_PRINCIPAL_30.x/table.2.reLoan$LOAN_PR, d = 2)
    
    table.2.reLoan$BNRatio <- percent(table.2.reLoan$OD_PRINCIPAL_30.y/table.2.reLoan$LOAN_PR, d = 2)
  }
  # table.2.part.3.reLoan <- aggregate(BAL~DATA_DT, subset(data, RELOAN != 1), FUN = "sum")
  # 
  # table.2.part.4.reLoan <- aggregate(OD_AMT~DATA_DT, subset(data, RELOAN != 1), FUN = "sum")
  # 
  # table.2.part.5.reLoan <- aggregate(OD_AMT~DATA_DT, subset(data, RELOAN != 1 & MATURITY_DAYS >= 3), FUN = "sum")
  # 
  # table.2.reLoan <- Reduce(function(x,y) merge(x,y, by = "DATA_DT", all = T), list(table.2.part.1.reLoan, table.2.part.2.reLoan,
  #                                                                                  table.2.part.3.reLoan, table.2.part.4.reLoan,
  #                                                                                  table.2.part.5.reLoan))
  # 
  # table.2.reLoan$Ratio <- percent(table.2.reLoan$OD_AMT.x/table.2.reLoan$BAL, d = 2)
  # 
  # table.2.reLoan$BNRatio <- percent(table.2.reLoan$OD_AMT.y/table.2.reLoan$BAL, d = 2)
  
  # all
  table.2.part.1.allLoan <- aggregate(CNT~DATA_DT, subset(data, OVERDUE_STATUS_3_30 != 2), FUN = "sum")
  
  table.2.part.2.allLoan <- aggregate(CNT~DATA_DT, subset(data, OVERDUE_STATUS_3_30 == 1), FUN = "sum")
  
  if(type == "interest"){
    table.2.part.3.allLoan <- aggregate(OD_AMT_30~DATA_DT, data, FUN = "sum")
    
    # table.2.part.4.allLoan <- aggregate(OD_AMT_30~DATA_DT, subset(data, MATURITY_DAYS > 0), FUN = "sum")
    
    table.2.part.5.allLoan <- aggregate(OD_AMT_30~DATA_DT, subset(data, MATURITY_DAYS >= 3), FUN = "sum")
    
    table.2.part.6.allLoan <- aggregate(LOAN_PR_FEE~DATA_DT, data, FUN = "sum")
    
    table.2.allLoan <- Reduce(function(x,y) merge(x,y, by = "DATA_DT", all = T), list(table.2.part.1.allLoan, table.2.part.2.allLoan,
                                                                                      table.2.part.3.allLoan, # table.2.part.4.allLoan,
                                                                                      table.2.part.5.allLoan, table.2.part.6.allLoan))
    table.2.allLoan[is.na(table.2.allLoan)] <- 0
    table.2.allLoan$ODRatio <- percent(table.2.allLoan$OD_AMT_30.x/table.2.allLoan$LOAN_PR_FEE, d= 2)
    # table.2.allLoan$BORatio <- percent(table.2.allLoan$OD_AMT_30.y/table.2.allLoan$LOAN_PR_FEE, d = 2)
    table.2.allLoan$BNRatio <- percent(table.2.allLoan$OD_AMT_30.y/table.2.allLoan$LOAN_PR_FEE, d = 2)
  }else if(type == "withoutInterest"){
    table.2.part.3.allLoan <- aggregate(OD_PRINCIPAL_30~DATA_DT, data, FUN = "sum")
    
    # table.2.part.4.allLoan <- aggregate(OD_PRINCIPAL_30~DATA_DT, subset(data, MATURITY_DAYS > 0), FUN = "sum")
    
    table.2.part.5.allLoan <- aggregate(OD_PRINCIPAL_30~DATA_DT, subset(data, MATURITY_DAYS >= 3), FUN = "sum")
    
    table.2.part.6.allLoan <- aggregate(LOAN_PR~DATA_DT, data, FUN = "sum")
    
    table.2.allLoan <- Reduce(function(x,y) merge(x,y, by = "DATA_DT", all = T), list(table.2.part.1.allLoan, table.2.part.2.allLoan,
                                                                                      table.2.part.3.allLoan, # table.2.part.4.allLoan,
                                                                                      table.2.part.5.allLoan, table.2.part.6.allLoan))
    table.2.allLoan[is.na(table.2.allLoan)] <- 0
    table.2.allLoan$ODRatio <- percent(table.2.allLoan$OD_PRINCIPAL_30.x/table.2.allLoan$LOAN_PR, d= 2)
    # table.2.allLoan$BORatio <- percent(table.2.allLoan$OD_PRINCIPAL_30.y/table.2.allLoan$LOAN_PR, d = 2)
    table.2.allLoan$BNRatio <- percent(table.2.allLoan$OD_PRINCIPAL_30.y/table.2.allLoan$LOAN_PR, d = 2)
  }
  # table.2.part.3.allLoan <- aggregate(OD_AMT~DATA_DT, data, FUN = "sum")
  # 
  # table.2.part.4.allLoan <- aggregate(OD_AMT~DATA_DT, subset(data, MATURITY_DAYS > 0), FUN = "sum")
  # 
  # table.2.part.5.allLoan <- aggregate(OD_AMT~DATA_DT, subset(data, MATURITY_DAYS >= 3), FUN = "sum")
  # 
  # table.2.part.6.allLoan <- aggregate(BAL~DATA_DT, data, FUN = "sum")
  # 
  # table.2.allLoan <- Reduce(function(x,y) merge(x,y, by = "DATA_DT", all = T), list(table.2.part.1.allLoan, table.2.part.2.allLoan,
  #                                                                                   table.2.part.3.allLoan, table.2.part.4.allLoan,
  #                                                                                   table.2.part.5.allLoan, table.2.part.6.allLoan))
  # table.2.allLoan$ODRatio <- percent(table.2.allLoan$OD_AMT.x/table.2.allLoan$BAL, d= 2)
  # table.2.allLoan$BORatio <- percent(table.2.allLoan$OD_AMT.y/table.2.allLoan$BAL, d = 2)
  # table.2.allLoan$BNRatio <- percent(table.2.allLoan$OD_AMT/table.2.allLoan$BAL, d = 2)
  
  table.2 <- Reduce(function(x,y) merge(x,y, by = "DATA_DT", all = T), list(table.2.firstLoan, table.2.reLoan, table.2.allLoan))
  
  table.2$DATA_DT <- substr(table.2$DATA_DT, 1, 7)
  
  return(table.2)
}

capitalEval <- function(data, type){
  # 资产情况月度分析  全量 Output: table.5.1; table.5.2
  # data.table.5 <- arrange(risk.data.all[c("DATA_DT", "BEGIN_DATE", "od_amt", "LOAN_M_CNT", "NEW_AMT")], BEGIN_DATE)
  data$LOAN_M_CNT <- substr(data$LOAN_M_CNT, 1, 7)

  table.5.1.1 <- arrange(aggregate(NEW_AMT~BEGIN_DATE, data, FUN = "sum"), BEGIN_DATE)

  if(type == "interest"){
    table.5.1.2 <- arrange(aggregate(OD_AMT_30~BEGIN_DATE+LOAN_M_CNT,
                                     subset(data, BEGIN_DATE >= min(data$DATA_DT)), FUN = "sum"), BEGIN_DATE, as.numeric(LOAN_M_CNT)) %>%
      reshape(idvar = "BEGIN_DATE", timevar = "LOAN_M_CNT", direction = "wide")
  } else if(type == "withoutInterest"){
    table.5.1.2 <- arrange(aggregate(OD_PRINCIPAL_30~BEGIN_DATE+LOAN_M_CNT,
                                     subset(data, BEGIN_DATE >= min(data$DATA_DT)), FUN = "sum"), BEGIN_DATE, as.numeric(LOAN_M_CNT)) %>%
      reshape(idvar = "BEGIN_DATE", timevar = "LOAN_M_CNT", direction = "wide")
  }
  
  table.5.1 <- merge(table.5.1.1, table.5.1.2, all.y = T)


  table.5.2 <- cbind(table.5.1[,1:2], table.5.1[,-1:-2]/table.5.1$NEW_AMT)
  table.5.2[,-1:-2] <- apply(table.5.2[,-1:-2], c(1,2), function(x) if(!(is.na(x))) percent(x, d = 2) else NA)

  table.5.1$BEGIN_DATE <- substr(table.5.1$BEGIN_DATE, 1, 7)
  table.5.2$BEGIN_DATE <- substr(table.5.2$BEGIN_DATE, 1, 7)
  
  if(type == "interest"){
    name.table.5 <- as.numeric(substr(colnames(table.5.1)[-1:-3], 11, nchar(colnames(table.5.1)[-1:-3]))) + 1
  } else if(type == "withoutInterest"){
    name.table.5 <- as.numeric(substr(colnames(table.5.1)[-1:-3], 17, nchar(colnames(table.5.1)[-1:-3]))) + 1
  }
  # name.table.5 <- as.numeric(substr(colnames(table.5.1)[-1:-3], 17, nchar(colnames(table.5.1)[-1:-3]))) + 1
  name.table.5 <- Reduce(function(x,y) paste(x,y, sep = ""), list("第", name.table.5, "个月"))
  colnames(table.5.1) <- c("新增放款月份", "新增放款金额", "当月", name.table.5)
  colnames(table.5.2) <- colnames(table.5.1)

  # Output: table.4.1; table.4.2 续贷
  # data <- arrange(data[c("DATA_DT", "BEGIN_DATE", "OA_AMT_30", "OD_PRINCIPAL_30", "LOAN_M_CNT", "NEW_AMT", "BRELOAN")], BEGIN_DATE)
  data$LOAN_M_CNT <- substr(data$LOAN_M_CNT, 1, 7)

  table.4.1.1 <- arrange(aggregate(NEW_AMT~BEGIN_DATE, subset(data, BRELOAN != 1), FUN = "sum"), BEGIN_DATE)

  if(type == "interest"){
    table.4.1.2 <- arrange(aggregate(OD_AMT_30~BEGIN_DATE+LOAN_M_CNT,
                                     subset(data, BEGIN_DATE >= min(data$DATA_DT) & BRELOAN != 1), FUN = "sum"), as.numeric(LOAN_M_CNT)) %>%
      # BEGIN_DATE, as.numeric(LOAN_M_CNT)) %>%
      reshape(idvar = "BEGIN_DATE", timevar = "LOAN_M_CNT", direction = "wide")
  } else if(type == "withoutInterest"){
    table.4.1.2 <- arrange(aggregate(OD_PRINCIPAL_30~BEGIN_DATE+LOAN_M_CNT,
                                     subset(data, BEGIN_DATE >= min(data$DATA_DT) & BRELOAN != 1), FUN = "sum"), as.numeric(LOAN_M_CNT)) %>%
      # BEGIN_DATE, as.numeric(LOAN_M_CNT)) %>%
      reshape(idvar = "BEGIN_DATE", timevar = "LOAN_M_CNT", direction = "wide")
  }

  table.4.1 <- merge(table.4.1.1, table.4.1.2, all.y = T)


  table.4.2 <- cbind(table.4.1[,1:2], table.4.1[,-1:-2]/table.4.1$NEW_AMT)
  table.4.2[,-1:-2] <- apply(table.4.2[,-1:-2], c(1,2), function(x) if(!(is.na(x))) percent(x, d = 2) else NA)

  table.4.1$BEGIN_DATE <- substr(table.4.1$BEGIN_DATE, 1, 7)
  table.4.2$BEGIN_DATE <- substr(table.4.2$BEGIN_DATE, 1, 7)
  
  if(type == "interest"){
    name.table.4 <- as.numeric(substr(colnames(table.4.1)[-1:-3], 11, nchar(colnames(table.4.1)[-1:-3]))) + 1
  } else if(type == "withoutInterest"){
    name.table.4 <- as.numeric(substr(colnames(table.4.1)[-1:-3], 17, nchar(colnames(table.4.1)[-1:-3]))) + 1
  }
  
  name.table.4 <- Reduce(function(x,y) paste(x,y, sep = ""), list("第", name.table.4, "个月"))
  colnames(table.4.1) <- c("新增放款月份", "新增放款金额", "当月", name.table.4)
  colnames(table.4.2) <- colnames(table.4.1)
  
  return(list(table.5.1, table.5.2, table.4.1, table.4.2))
}


all.interest.overdue <- overdueIndex(risk.data.all, "interest")
all.withoutInterest.overdue <- overdueIndex(risk.data.all, "withoutInterest")
exp.interest.overdue <- overdueIndex(risk.data.exp, "interest")
exp.withoutInterest.overdue <- overdueIndex(risk.data.exp, "withoutInterest")

all.interest.capital <- capitalEval(risk.data.all, "interest")
all.withoutInterest.capital <- capitalEval(risk.data.all, "withoutInterest")
exp.interest.capital <- capitalEval(risk.data.exp, "interest")
exp.withoutInterest.capital <- capitalEval(risk.data.exp, "withoutInterest")

ExcelFile <- "D:\\Allinpay\\Data\\TeamWork\\dataForReport\\template_M30.xlsx"
template <- paste0("D:\\Allinpay\\Data\\TeamWork\\dataForReport\\M30_", Sys.Date(), ".xlsx")
file.copy(ExcelFile, template)



writeWorksheetToFile(template, all.interest.overdue,
                     sheet = "逾期情况1", header = F,
                     startRow = 4, startCol = 1)
writeWorksheetToFile(template, all.withoutInterest.overdue,
                     sheet = "逾期情况2", header = F,
                     startRow = 4, startCol = 1)
writeWorksheetToFile(template, exp.interest.overdue,
                     sheet = "逾期情况3", header = F,
                     startRow = 4, startCol = 1)
writeWorksheetToFile(template, exp.withoutInterest.overdue,
                     sheet = "逾期情况4", header = F,
                     startRow = 4, startCol = 1)

list.output <- function(data, sheetName){
  r <- 3
  for(i in 1:length(data)){
    if(i == 3){
      writeWorksheetToFile(template, data = c("续贷资产情况"),
                           sheet = sheetName, header = F,
                           startRow = r - 1, startCol = 1)
    }
    writeWorksheetToFile(template, data[[i]],
                         sheet = sheetName, header = T,
                         startRow = r, startCol = 1)
    r <- r + nrow(data[[i]]) + 3
  }
}

list.output(all.interest.capital, "资产情况1")
list.output(all.withoutInterest.capital, "资产情况2")
list.output(exp.interest.capital, "资产情况3")
list.output(exp.withoutInterest.capital, "资产情况4")




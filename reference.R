# part.1  包介绍
# 首次使用需安装    install.packages("RODBC")
# 使用前需载入      library(RODBC)
# 
# RODBC     连接数据库
# dplyr     arrange排序
# tidyr     unite连接
# lubridate 截取年月日
# sca       percent控制精度
# XLConnect 输出至excel
# 
# 
# part.2  各函数使用(F1查询各函数说明)
# subset(data, logicalExpression)                    获取子集
# arrange(data, col1, dec(col2), ...)                按col1升序,col2降序等为data排序
# unite(data, Newname, col1, col2, ..., sep)         以sep标识符合成data中的col1, col2等，新列命名Newname
# aggregate(Y, by, FUN)                              按by条件(单条件或多添加)对Y(多目标)分组，对每组实现FUN功能
# aggregate(y ~ X, data, FUN)                        按X条件(单条件或多条件)对data中的y(单目标)分组,对每组实现FUN功能
# reshape(data, idvar, timevar, direction)           数据重组
# append(x, values, after)                           在x的after位置后插入values
# percent(data, d)                                   将data转化为精度为d的百分数
# substr(x, start, stop)                             截取x从start到stop段的字符
# merge(x, y, by.x, by.y)                            按x的by.x列及y的by.y列对x,y进行合并
# apply(x, margin, FUN)                              对x的margin(1为每行,2为每列,c(1,2)为每个元素)进行FUN操作


library(RODBC)
library(dplyr)
library(tidyr)
library(lubridate)
library(sca)
library(XLConnect)

channel <- odbcConnect("OracleInstantClient", uid = "zhangj", pwd = "zhangj1234")
reference.data <- sqlQuery(channel, 'select * from THBL.RISK_STATISTICS_ALL')

subset(reference.data, OVERDUE_STATUS_5 == 0 & OVERDUE_STATUS_5_LAST == 3)

arrange(reference.data, OVERDUE_STATUS_5, OVERDUE_STATUS_5_LAST)

unite(reference.data, uniteCol, OVERDUE_STATUS_5_LAST, OVERDUE_STATUS_5, sep = "-")

aggregate(reference.data[c("CNT", "OD_AMT")],by = list(DATA_DT = reference.data$DATA_DT, PROV  = reference.data$PROV_CD), "sum")

aggregate(CNT ~ DATA_DT + PROV_CD, reference.data, "sum")

reshape(aggregate(CNT ~ DATA_DT + PROV_CD, reference.data, "sum"), idvar = "DATA_DT", timevar = "PROV_CD", direction = "wide")







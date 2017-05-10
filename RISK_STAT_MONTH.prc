CREATE OR REPLACE PROCEDURE "RISK_STAT_MONTH"(DATA_DT in varchar2) AS
BEGIN
DECLARE
 T_DT              VARCHAR2(10):=DATA_DT;
 MON_FIRST_DT      VARCHAR2(10);    --�µ�һ��
 LAST_MON_LAST_DT      VARCHAR2(10);    --�������һ��
 YEAR_FIRST_DT      VARCHAR2(10);    --���һ��
 LAST_YEAR_LAST_DT      VARCHAR2(10);    --�������һ��
 NEXT_DT                VARCHAR2(10);     --��һ��
 LAST_DT             VARCHAR2(10);          --ǰһ��

BEGIN
------------�µ�һ��
SELECT  to_char(trunc(add_months(last_day(TO_DATE(T_DT,'YYYYMMDD')), -1) + 1), 'yyyymmdd') 
INTO MON_FIRST_DT
FROM DUAL;   
------------�������һ��
SELECT to_char(trunc(add_months(last_day(TO_DATE(T_DT,'YYYYMMDD')), -1)), 'yyyymmdd')
INTO LAST_MON_LAST_DT
FROM DUAL;  
------------���һ��
select TO_CHAR(trunc(TO_DATE(T_DT,'YYYYMMDD'),'yy'),'yyyymmdd')
INTO YEAR_FIRST_DT
from dual;
------------�������һ��  
SELECT  TO_CHAR(trunc(TO_DATE(T_DT,'YYYYMMDD'),'year')-1,'yyyymmdd')
INTO LAST_YEAR_LAST_DT
FROM DUAL;
-------------��һ��
SELECT TO_CHAR(TO_DATE(T_DT,'YYYYMMDD')+1,'yyyymmdd')
INTO NEXT_DT
FROM DUAL;
------------ǰһ��
SELECT TO_CHAR(TO_DATE(T_DT,'YYYYMMDD')-1,'yyyymmdd')
INTO LAST_DT
FROM DUAL;

EXECUTE IMMEDIATE 'TRUNCATE TABLE all_loan';
INSERT INTO all_loan select * from v_all_loan;

-- drop table temp_risk_stat purge;
DELETE FROM RISK_STAT_DETAIL WHERE DATA_DT = TO_DATE(LAST_MON_LAST_DT,'YYYYMMDD');
INSERT INTO RISK_STAT_DETAIL
SELECT
val.mcht_cd,
val.duebillno as db_no, -- key
val.data_dt as data_dt,
case when a.OVERDUE_STATUS='����' then 0
     when a.overdue_status='��ע' then 1
     when a.overdue_status='һ��' then 2
     when a.overdue_status='����' then 3
     when a.overdue_status='����' then 4 end as overdue_status_5, -- od 5
-- '' as overdue_status_5_last, -- ���� od 5 
case when a.OVERDUE_STATUS in ('����','��ע') then 0
     when a.OVERDUE_STATUS in ('һ��','����','����') then 1
     when val.flag = 'D0010' THEN 2 end as overdue_status_3, -- ���� ���� ����
-- '' as overdue_status_3_last,
case when trunc(a.data_dt,'MM') = trunc(a.loan_date,'MM') then 1 else 0 end as new_loan,  -- 1 ��������  0 ��ʷ���� (��������)RISK_STATISTICS_ALL.BEGIN_DATE
count(val.mcht_cd) over(partition by val.mcht_cd,val.data_dt) as reloan, -- �̻��������(����)
--'' as status_last_month,
case when STATUS='�״̬(active)' then 0
     when STATUS='��ֹ(terminate)' then 1
     when STATUS='���(finish)' then 2
     end as status_this_month,   -- od 3 �״̬(active)   ��ֹ(terminate)     ���(finish)
case when (val.data_dt - to_date(val.loan_maturity_date,'YYYY-MM-DD')) <=0 then -1
     when (val.data_dt -  to_date(val.loan_maturity_date,'YYYY-MM-DD')) >0 and (val.data_dt - to_date(val.loan_maturity_date,'YYYY-MM-DD')) <=30 then 0 
     when (val.data_dt -  to_date(val.loan_maturity_date,'YYYY-MM-DD')) >30 and (val.data_dt - to_date(val.loan_maturity_date,'YYYY-MM-DD')) <=60 then 1 
     when (val.data_dt -  to_date(val.loan_maturity_date,'YYYY-MM-DD')) >60 and (val.data_dt - to_date(val.loan_maturity_date,'YYYY-MM-DD')) <=90 then 2 
     when (val.data_dt -  to_date(val.loan_maturity_date,'YYYY-MM-DD')) >90 then 3 
     end as maturity_days,
case when a.overdue_days <=0 then -1
     when a.overdue_days >0 and a.overdue_days <=30 then 0
     when a.overdue_days >30 and a.overdue_days <=60 then 1
     when a.overdue_days >60 and a.overdue_days <=90 then 2
     when a.overdue_days >90 and a.overdue_days <=180 then 3
     when a.overdue_days >180 then 4 
     end as overdue_days,
case when a.period_type = '��' and a.overdue_days < 8 then 0
     when a.period_type = '��' and a.overdue_days <8 then 0
     when a.period_type = '˫��' and a.overdue_days < 15 then 0
     when a.period_type = '��' and a.overdue_days >= 8 and a.overdue_days<=30 then 1
     when a.period_type = '��' and a.overdue_days >=8 and a.overdue_days<=30 then 1
     when a.period_type = '˫��' and a.overdue_days >= 15 and a.overdue_days<=30 then 1
     when a.overdue_days>30 and a.overdue_days<=60 then 2
     when a.overdue_days>60 and a.overdue_days<=90 then 3
     when a.overdue_days>90 and a.overdue_days<=180 then 4   
     when a.overdue_days>180 and a.overdue_days<=270 then 5
     when a.overdue_days>270 then 6
     end as overdue_flag,
add_months(trunc(to_date(val.loan_date,'YYYY-MM-DD'),'month'),1) -1 as begin_date,    -- ��Ϣ���� 
-- trunc(to_date(val.loan_date,'YYYY-MM-DD'),'MM') as begin_date,   
1 as cnt,   -- ����
nvl(a.LOAN_BALANCE,0) as bal,   -- ���
case when a.OVERDUE_STATUS in ('����','��ע') or a.OVERDUE_STATUS is null then 0 else (a.SP_PRINCIPAL+a.SP_FEE+a.SP_FEE1+a.SP_FEE2) end as od_amt, -- ���ڽ��
val.allap_sum as ap_amt,  -- �ۼ��ѻ����
val.allsp_sum as sp_amt,  -- �ۼ�Ӧ�����
case when trunc(val.data_dt,'MM') = trunc(to_date(val.loan_date,'YYYY-MM-DD'),'MM') then val.loan_pr else 0 end as new_amt, -- ���������ſ���
case when a.OVERDUE_STATUS in ('����','��ע') or a.OVERDUE_STATUS is null then 0 else (a.SP_PRINCIPAL) end as od_principal, -- ���ڱ���
c.CODE_4 as shengshi_cd,
months_between(trunc(val.data_dt,'MM'),trunc(to_date(val.loan_date,'YYYY-MM-DD'),'MM')) as loan_m_cnt,   -- �����n����
case when c.code_4 in ('4403','3702','3502','3302','2102') then c.code_4 else x.prov end as prov_cd,
x.city as city_cd,
val.loan_pr AS loan_pr, -- ������
(row_number() over(partition by val.mcht_cd,val.data_dt order by val.loan_date)) as reloantimes, -- �����˴�
case when x.APPLY_REASON = 'P' then 0 else 1 end as light, -- P 0 �̵ơ��� 1�Ƶ�
case when val.loan_pr > 0 and val.loan_pr <= 100000 then 0
     when  val.loan_pr > 100000 and val.loan_pr <= 200000 then 1
     when  val.loan_pr > 200000 and val.loan_pr <= 300000 then 2
     when  val.loan_pr > 300000 then 3
    end as loan_pr_scope
from all_loan val
left join T0004_AFTERLOANRPT a
on val.duebillno = a.db_no and val.data_dt = a.data_dt
LEFT JOIN v_recent_sign X 
    on x.mcht_cd = val.mcht_cd and val.DATA_DT = x.DATA_DT
LEFT JOIN T99_REG_CORP C
   ON x.prov||x.city = c.code_4  
where val.data_dt = TO_DATE(LAST_MON_LAST_DT,'YYYYMMDD')
/*
in (
to_date('201602','YYYYMM')-1,
to_date('201603','YYYYMM')-1,
to_date('201604','YYYYMM')-1,
to_date('201605','YYYYMM')-1,
to_date('201606','YYYYMM')-1,
to_date('201607','YYYYMM')-1,
to_date('201608','YYYYMM')-1,
to_date('201609','YYYYMM')-1,
to_date('201610','YYYYMM')-1,
to_date('201611','YYYYMM')-1,
to_date('201612','YYYYMM')-1,
to_date('201701','YYYYMM')-1,
to_date('201702','YYYYMM')-1,
to_date('201703','YYYYMM')-1,
to_date('201704','YYYYMM')-1,
to_date('201705','YYYYMM')-1,
to_date('201706','YYYYMM')-1,
to_date('201707','YYYYMM')-1,
to_date('201708','YYYYMM')-1,
to_date('201709','YYYYMM')-1,
to_date('201710','YYYYMM')-1)
-- and val.mcht_cd not in ('821530173990206','821530173990264')
*/;

EXECUTE IMMEDIATE 'TRUNCATE table RISK_STAT_L_DETAIL';
INSERT INTO RISK_STAT_L_DETAIL 
SELECT
    t.mcht_cd,
    t.DB_NO,
    t.DATA_DT,
    t.OVERDUE_STATUS_5,
    r.OVERDUE_STATUS_5 as overdue_status_5_last, -- ���� od 5 
    t.OVERDUE_STATUS_3,
    r.OVERDUE_STATUS_3 as overdue_status_3_last,
    t.NEW_LOAN,
    t.RELOAN,
    r.STATUS_THIS_MONTH as status_last_month,
    t.STATUS_THIS_MONTH,
    t.MATURITY_DAYS,
    t.OVERDUE_DAYS,
    t.OVERDUE_FLAG,
    t.BEGIN_DATE,
    t.CNT,
    t.BAL,
    t.OD_AMT,
    t.AP_AMT,
    t.SP_AMT,
    t.NEW_AMT,
    t.OD_PRINCIPAL,
    t.SHENGSHI_CD,
    nvl(t.od_amt,0)-nvl(r.od_amt,0) as diff_od_amt,
    nvl(t.AP_AMT,0)-nvl(r.AP_AMT,0) as diff_ap_amt,
    nvl(t.SP_AMT,0)-nvl(r.SP_AMT,0) as diff_sp_amt,
    t.loan_m_cnt,
    t.prov_cd,
    t.city_cd,
    t.loan_pr,
    r.loan_pr as loan_pr_last,
    t.reloantimes,
    t.light,
    t.loan_pr_scope
FROM
    RISK_STAT_DETAIL t 
LEFT JOIN RISK_STAT_DETAIL r
    on t.DATA_DT = add_months(r.DATA_DT,1) and t.DB_NO = r.DB_NO;
    
update RISK_STAT_L_DETAIL set 
overdue_status_5 = 0,
overdue_status_5_last=0,
overdue_status_3=0,
overdue_status_3_last=0,
maturity_days=-1,
overdue_days=-1
,od_amt=0
,diff_od_amt=0
,od_principal=0
 where db_no in ('68950000000000003922','68950000000000003614','68950000000000000716') and overdue_status_3 <> 2 ;

EXECUTE IMMEDIATE 'truncate table RISK_STATISTICS_ALL';
insert into THBL.RISK_STATISTICS_ALL
SELECT
    t.DATA_DT,
    t.OVERDUE_STATUS_5,
    t.OVERDUE_STATUS_5_LAST,
    t.OVERDUE_STATUS_3,
    t.OVERDUE_STATUS_3_LAST,
    t.NEW_LOAN,
    t.RELOAN,
    t.STATUS_LAST_MONTH,
    t.STATUS_THIS_MONTH,
    t.MATURITY_DAYS,
    t.OVERDUE_DAYS,
    t.OVERDUE_FLAG,
    t.BEGIN_DATE,
    sum(t.CNT),
    sum(t.BAL),
    sum(t.OD_AMT),
    sum(t.AP_AMT),
    sum(t.SP_AMT),
    sum(t.NEW_AMT),
    sum(t.OD_PRINCIPAL),
    t.SHENGSHI_CD,
    sum(t.diff_od_amt),
    sum(diff_ap_amt),
    sum(diff_sp_amt),
    t.loan_m_cnt,
    t.prov_cd,
    t.city_cd,
    sum(t.loan_pr),
    sum(t.loan_pr_last),
    reloantimes,
    light,
    t.loan_pr_scope,
    COUNT(DISTINCT MCHT_CD)
FROM
    risk_stat_L_DETAIL t
GROUP BY
    t.DATA_DT,
    t.OVERDUE_STATUS_5,
    t.OVERDUE_STATUS_5_LAST,
    t.OVERDUE_STATUS_3,
    t.OVERDUE_STATUS_3_LAST,
    t.NEW_LOAN,
    t.RELOAN,
    t.STATUS_LAST_MONTH,
    t.STATUS_THIS_MONTH,
    t.MATURITY_DAYS,
    t.OVERDUE_DAYS,
    t.OVERDUE_FLAG,
    t.BEGIN_DATE,
    t.SHENGSHI_CD,
    t.loan_m_cnt,
    t.prov_cd,
    t.city_cd,
    t.reloantimes,
    t.light,
    t.loan_pr_scope;

 
COMMIT;
END;
END RISK_STAT_MONTH;
/

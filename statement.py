"""
Statement module
"""

ct_orb = """CREATE TABLE orabase (
  emplid text,
  adm_appl_nbr text,
  admit_type text,
  academic_level text,
  admit_term text,
  acad_prog text,
  acad_plan text,
  prog_action text,
  prog_reason text,
  appl_fee_status text
)"""

ct_orx1 = """CREATE TABLE oraaux1 (
  emplid text,
  acad_career text,
  stdnt_car_nbr int,
  adm_appl_nbr text,
  appl_prog_nbr int,
  effdt text,
  effseq int,
  institution text,
  acad_prog text,
  prog_status text,
  prog_action text,
  action_dt text,
  prog_reason text,
  admit_term text,
  exp_grad_term text,
  req_term text,
  acad_load_appr text,
  campus text,
  acad_prog_dual text,
  joint_prog_appr text,
  ssr_rs_candit_nbr text,
  ssr_apt_instance int,
  ssr_yr_of_prog text,
  ssr_shift text,
  ssr_cohort_id text,
  scc_row_add_oprid text,
  scc_row_add_dttm text,
  scc_row_upd_oprid text,
  scc_row_upd_dttm text,
  Xemplid text,
  Xacad_career text,
  Xstdnt_car_nbr int,
  Xadm_appl_nbr text,
  Xappl_prog_nbr int,
  Xeffdt text,
  Xeffseq int,
  Xacad_plan text,
  Xdeclare_dt text,
  Xplan_sequence int,
  Xreq_term text,
  Xssr_apt_instance int,
  Xssr_yr_of_prog text,
  Xscc_row_add_oprid text,
  Xscc_row_add_dttm text,
  Xscc_row_upd_oprid text,
  Xscc_row_upd_dttm text
)"""

ct_orx2 = """CREATE TABLE oraaux2 (
  emplid text,
  preferred_name text,
  name_prefix text,
  first_name text,
  middle_name text,
  last_name int,
  name_suffix text,
  home_phone text,
  cell_phone text,
  phone_pref text,
  other_email text,
  email_pref text,
  home_country text,
  home_street text,
  home_city text,
  home_county text,
  home_state text,
  home_postal text,
  mail_country text,
  mail_street text,
  mail_city text,
  mail_county text,
  mail_state text,
  mail_postal text
)"""

ct_msb = """CREATE TABLE mssbase (
  emplid text,
  adm_appl_nbr text,
  admit_type text,
  academic_level text,
  admit_term text,
  acad_prog text,
  acad_plan text,
  prog_action text,
  prog_reason text,
  appl_fee_status text
)"""

ct_msx1 = """CREATE TABLE mssaux1 (
  emplid text,
  preferred text,
  primary_phone text,
  mobile_phone text,
  email text,
  permanent_country text,
  permanent_street text,
  permanent_city text,
  permanent_county text,
  permanent_region text,
  permanent_postal text,
  mailing_country text,
  mailing_street text,
  mailing_city text,
  mailing_county text,
  mailing_region text,
  mailing_postal text
)"""

qi_msb = """select
  (select [value] from dbo.getFieldTopTable(p.[id], 'emplid')) as [EMPLID],
  (select [value] from dbo.getFieldTopTable(a.[id], 'adm_appl_nbr')) as [ADM_APPL_NBR],
  (select top 1 [value] from dbo.getFieldExportTable(a.[id], 'ug_appl_admit_type')) as [ADMIT_TYPE],
  (select top 1 [value] from dbo.getFieldExportTable(a.[id], 'ug_appl_academic_level')) as [ACADEMIC_LEVEL],
  (select top 1 [value] from dbo.getFieldExportTable(a.[id], 'appl_admit_term')) as [ADMIT_TERM],
  coalesce((select top 1 [value] from dbo.getFieldTopTable(a.[id], 'ug_appl_acad_prog_pending')), (select top 1 [value] from dbo.getFieldTopTable(a.[id], 'ug_appl_acad_prog'))) as [ACAD_PROG],
  coalesce((select top 1 [value] from dbo.getFieldExportTable(a.[id], 'ug_appl_acad_plan_pending')), (select top 1 [value] from dbo.getFieldExportTable(a.[id], 'ug_appl_acad_plan'))) as [ACAD_PLAN],
  (select top 1 [value] from dbo.getFieldExportTable(a.[id], 'prog_action')) as [PROG_ACTION],
  (select top 1 [value] from dbo.getFieldExportTable(a.[id], 'prog_reason')) as [PROG_REASON],
  (select top 1 [value] from dbo.getFieldTopTable(a.[id], 'appl_fee_status')) as [APPL_FEE_STATUS]
from [application] as a
inner join [person] as p on a.[person] = p.[id]
inner join [lookup.round] as lr on a.[round] = lr.[id]
inner join [lookup.period] as lp on lr.[period] = lp.[id]
where p.[id] not in (select [record] from [tag] where ([tag] in ('test')))
and lr.[key] != 'GR'
and a.[submitted] is not null
and lp.[active] = 1
order by 1, 2"""

qi_msx1 = """select distinct
  (select [value] from dbo.getFieldTopTable(p.[id], 'emplid')) as [EMPLID],
  p.[preferred] as [PREFERRED],
  (case
  when len(p.[phone]) = 15 and substring(p.[phone], 1, 3) = '+1 ' then convert(varchar(32), replace(right(p.[phone], 12), '-', ''))
  else p.[phone]
  end) as [PRIMARY_PHONE],
  (case
  when len(p.[mobile]) = 15 and substring(p.[mobile], 1, 3) = '+1 ' then convert(varchar(32), replace(right(p.[mobile], 12), '-', ''))
  else p.[mobile]
  end) as [MOBILE_PHONE],
  p.[email] as [EMAIL],
  padw.[alpha3] as [PERMANENT_COUNTRY],
  pad.[street] as [PERMANENT_STREET],
  pad.[city] as [PERMANENT_CITY],
  pad.[county] as [PERMANENT_COUNTY],
  pad.[region] as [PERMANENT_REGION],
  pad.[postal] as [PERMANENT_POSTAL],
  madw.[alpha3] as [MAILING_COUNTRY],
  mad.[street] as [MAILING_STREET],
  mad.[city] as [MAILING_CITY],
  mad.[county] as [MAILING_COUNTY],
  mad.[region] as [MAILING_REGION],
  mad.[postal] as [MAILING_POSTAL]
from [application] as a
inner join [person] as p on a.[person] = p.[id]
inner join [lookup.round] as lr on a.[round] = lr.[id]
inner join [lookup.period] as lp on lr.[period] = lp.[id]
left outer join [address] as pad on a.[person] = pad.[record] and pad.[type] = 'permanent' and pad.[rank] = 1 and pad.[country] in ('US', 'CA')
left outer join world.dbo.[country] as padw on pad.[country] = padw.[id]
left outer join [address] as mad on a.[person] = mad.[record] and mad.[type] is null and mad.[rank] = 1 and mad.[country] in ('US', 'CA')
left outer join world.dbo.[country] as madw on mad.[country] = madw.[id]
where p.[id] not in (select [record] from [tag] where ([tag] in ('test')))
and lr.[key] != 'GR'
and a.[submitted] is not null
and lp.[active] = 1
order by 1"""

qi_msx3 = """select distinct
  (select [value] from dbo.getFieldTopTable(p.[id], 'emplid')) as [EMPLID]
from [application] as a
inner join [person] as p on a.[person] = p.[id]
inner join [lookup.round] as lr on a.[round] = lr.[id]
inner join [lookup.period] as lp on lr.[period] = lp.[id]
where p.[id] not in (select [record] from [tag] where ([tag] in ('test')))
and lr.[key] != 'GR'
and a.[submitted] is not null
and lp.[active] = 1
and exists (
  select *
  from [field]
  where p.[id] = [record]
  and [field] = 'citizenship_status'
  and [prompt] = '01116174-db87-4115-8729-2785c7230017'
)
order by 1"""

qi_orb = """SELECT
  A.EMPLID,
  A.ADM_APPL_NBR,
  A.ADMIT_TYPE,
  A.ACADEMIC_LEVEL,
  A.ADMIT_TERM,
  A.ACAD_PROG,
  A.ACAD_PLAN,
  A.PROG_ACTION,
  A.PROG_REASON,
  A.APPL_FEE_STATUS
FROM PS_L_ADM_PROG_VW A
WHERE A.ADMIT_TERM BETWEEN :termlb AND :termub
AND A.ACAD_CAREER = 'UGRD'
ORDER BY A.EMPLID, A.ADM_APPL_NBR"""

qi_orx1 = """SELECT
  A.EMPLID,
  A.ACAD_CAREER,
  A.STDNT_CAR_NBR,
  A.ADM_APPL_NBR,
  A.APPL_PROG_NBR,
  TO_CHAR(A.EFFDT, 'YYYY-MM-DD'),
  A.EFFSEQ,
  A.INSTITUTION,
  A.ACAD_PROG,
  A.PROG_STATUS,
  A.PROG_ACTION,
  TO_CHAR(A.ACTION_DT, 'YYYY-MM-DD'),
  A.PROG_REASON,
  A.ADMIT_TERM,
  A.EXP_GRAD_TERM,
  A.REQ_TERM,
  A.ACAD_LOAD_APPR,
  A.CAMPUS,
  A.ACAD_PROG_DUAL,
  A.JOINT_PROG_APPR,
  A.SSR_RS_CANDIT_NBR,
  A.SSR_APT_INSTANCE,
  A.SSR_YR_OF_PROG,
  A.SSR_SHIFT,
  A.SSR_COHORT_ID,
  A.SCC_ROW_ADD_OPRID,
  A.SCC_ROW_ADD_DTTM,
  A.SCC_ROW_UPD_OPRID,
  A.SCC_ROW_UPD_DTTM,
  B.EMPLID,
  B.ACAD_CAREER,
  B.STDNT_CAR_NBR,
  B.ADM_APPL_NBR,
  B.APPL_PROG_NBR,
  TO_CHAR(B.EFFDT, 'YYYY-MM-DD'),
  B.EFFSEQ,
  B.ACAD_PLAN,
  TO_CHAR(B.DECLARE_DT, 'YYYY-MM-DD'),
  B.PLAN_SEQUENCE,
  B.REQ_TERM,
  B.SSR_APT_INSTANCE,
  B.SSR_YR_OF_PROG,
  B.SCC_ROW_ADD_OPRID,
  B.SCC_ROW_ADD_DTTM,
  B.SCC_ROW_UPD_OPRID,
  B.SCC_ROW_UPD_DTTM
FROM PS_ADM_APPL_PROG A
INNER JOIN PS_ADM_APPL_PLAN B ON A.EMPLID = B.EMPLID AND A.ACAD_CAREER = B.ACAD_CAREER AND A.STDNT_CAR_NBR = B.STDNT_CAR_NBR AND A.ADM_APPL_NBR = B.ADM_APPL_NBR AND A.APPL_PROG_NBR = B.APPL_PROG_NBR AND A.EFFDT = B.EFFDT AND A.EFFSEQ = B.EFFSEQ AND B.PLAN_SEQUENCE = 1
WHERE A.ADMIT_TERM BETWEEN :termlb AND :termub
AND A.ACAD_CAREER = 'UGRD'
AND A.EFFDT = (
  SELECT MAX(A_ED.EFFDT)
  FROM PS_ADM_APPL_PROG A_ED
  WHERE A.EMPLID = A_ED.EMPLID
  AND A.ACAD_CAREER = A_ED.ACAD_CAREER
  AND A.STDNT_CAR_NBR = A_ED.STDNT_CAR_NBR
  AND A.ADM_APPL_NBR = A_ED.ADM_APPL_NBR
  AND A.APPL_PROG_NBR = A_ED.APPL_PROG_NBR
) AND A.EFFSEQ = (
  SELECT MAX(A_ED.EFFSEQ)
  FROM PS_ADM_APPL_PROG A_ED
  WHERE A.EMPLID = A_ED.EMPLID
  AND A.ACAD_CAREER = A_ED.ACAD_CAREER
  AND A.STDNT_CAR_NBR = A_ED.STDNT_CAR_NBR
  AND A.ADM_APPL_NBR = A_ED.ADM_APPL_NBR
  AND A.APPL_PROG_NBR = A_ED.APPL_PROG_NBR
  AND A.EFFDT = A_ED.EFFDT
)
ORDER BY A.EMPLID, A.ADM_APPL_NBR"""

qi_orx2 = """SELECT DISTINCT
  A.EMPLID,
  B.FIRST_NAME AS "PREFERRED_NAME",
  BB.NAME_PREFIX,
  BB.FIRST_NAME,
  BB.MIDDLE_NAME,
  BB.LAST_NAME,
  BB.NAME_SUFFIX,
  REGEXP_REPLACE(C.PHONE, '[/-]') AS "HOME_PHONE",
  REGEXP_REPLACE(D.PHONE, '[/-]') AS "CELL_PHONE",
  E.PHONE_TYPE AS "PHONE_PREF",
  F.EMAIL_ADDR AS "OTHER_EMAIL",
  G.E_ADDR_TYPE AS "EMAIL_PREF",
  H.COUNTRY AS "HOME_COUNTRY",
  H.ADDRESS1 AS "HOME_STREET",
  H.CITY AS "HOME_CITY",
  H.COUNTY AS "HOME_COUNTY",
  H.STATE AS "HOME_STATE",
  H.POSTAL AS "HOME_POSTAL",
  I.COUNTRY AS "MAIL_COUNTRY",
  I.ADDRESS1 AS "MAIL_STREET",
  I.CITY AS "MAIL_CITY",
  I.COUNTY AS "MAIL_COUNTY",
  I.STATE AS "MAIL_STATE",
  I.POSTAL AS "MAIL_POSTAL"
FROM PS_L_ADM_PROG_VW A
LEFT OUTER JOIN PS_NAMES B ON A.EMPLID = B.EMPLID AND B.NAME_TYPE = 'PRF' AND B.EFFDT = (
    SELECT MAX(B_ED.EFFDT)
    FROM PS_NAMES B_ED
    WHERE B.EMPLID = B_ED.EMPLID
    AND B.NAME_TYPE = B_ED.NAME_TYPE
  ) AND B.EFF_STATUS = 'A'
LEFT OUTER JOIN PS_NAMES BB ON A.EMPLID = BB.EMPLID AND BB.NAME_TYPE = 'PRI' AND BB.EFFDT = (
    SELECT MAX(BB_ED.EFFDT)
    FROM PS_NAMES BB_ED
    WHERE BB.EMPLID = BB_ED.EMPLID
    AND BB.NAME_TYPE = BB_ED.NAME_TYPE
  ) AND BB.EFF_STATUS = 'A'
LEFT OUTER JOIN PS_PERSONAL_PHONE C ON A.EMPLID = C.EMPLID AND C.PHONE_TYPE = 'HOME'
LEFT OUTER JOIN PS_PERSONAL_PHONE D ON A.EMPLID = D.EMPLID AND D.PHONE_TYPE = 'CELL'
LEFT OUTER JOIN PS_PERSONAL_PHONE E ON A.EMPLID = E.EMPLID AND E.PREF_PHONE_FLAG = 'Y'
LEFT OUTER JOIN PS_EMAIL_ADDRESSES F ON A.EMPLID = F.EMPLID AND F.E_ADDR_TYPE = 'OTHR'
LEFT OUTER JOIN PS_EMAIL_ADDRESSES G ON A.EMPLID = G.EMPLID AND G.PREF_EMAIL_FLAG = 'Y'
LEFT OUTER JOIN PS_ADDRESSES H ON A.EMPLID = H.EMPLID AND H.ADDRESS_TYPE = 'HOME' AND H.COUNTRY IN ('USA', 'CAN') AND H.EFFDT = (
  SELECT MAX(H_ED.EFFDT)
  FROM PS_ADDRESSES H_ED
  WHERE H.EMPLID = H_ED.EMPLID
  AND H.ADDRESS_TYPE = H_ED.ADDRESS_TYPE
)
LEFT OUTER JOIN PS_ADDRESSES I ON A.EMPLID = I.EMPLID AND I.ADDRESS_TYPE = 'MAIL' AND I.COUNTRY IN ('USA', 'CAN') AND I.EFFDT = (
  SELECT MAX(I_ED.EFFDT)
  FROM PS_ADDRESSES I_ED
  WHERE I.EMPLID = I_ED.EMPLID
  AND I.ADDRESS_TYPE = I_ED.ADDRESS_TYPE
)
WHERE NOT EXISTS (
  SELECT *
  FROM PS_PERS_INST_REL XA
  WHERE A.EMPLID = XA.EMPLID
  AND XA.ALUMNI_CUR <> 'Y'
  AND XA.EMPLOYEE_CUR <> 'Y'
  AND XA.FIN_AID_CUR <> 'Y'
  AND XA.STUDENT_CUR <> 'Y'
  AND XA.STDNT_FIN_CUR <> 'Y'
  AND XA.RECRUITER_CUR <> 'Y'
  AND XA.ADVISOR_CUR <> 'Y'
  AND XA.INSTRCTOR_CUR <> 'Y'
  AND XA.FRIEND_CUR <> 'Y'
)
AND NOT EXISTS (
  SELECT *
  FROM PS_PERSON XA
  WHERE A.EMPLID = XA.EMPLID
  AND XA.DT_OF_DEATH IS NOT NULL
)
AND A.ADMIT_TERM BETWEEN :termlb AND :termub
AND A.ACAD_CAREER = 'UGRD'
AND NOT EXISTS (
    SELECT *
    FROM PS_L_ADM_PROG_VW XA
    WHERE A.EMPLID = XA.EMPLID
    AND XA.PROG_ACTION <> 'MATR'
)
ORDER BY A.EMPLID"""

qi_orr1 = """SELECT DISTINCT A.ACAD_PROG, B.ACAD_PLAN
FROM PS_ACAD_PROG_TBL A
INNER JOIN PS_ACAD_PLAN_TBL B ON A.ACAD_PROG = B.ACAD_PROG AND A.EFF_STATUS = B.EFF_STATUS AND B.EFFDT = (
  SELECT MAX(B_ED.EFFDT)
  FROM PS_ACAD_PLAN_TBL B_ED
  WHERE B.ACAD_PLAN = B_ED.ACAD_PLAN
)
WHERE A.EFF_STATUS = 'A'
AND A.ACAD_CAREER = 'UGRD'
AND A.EFFDT = (
  SELECT MAX(A_ED.EFFDT)
  FROM PS_ACAD_PROG_TBL A_ED
  WHERE A.ACAD_PROG = A_ED.ACAD_PROG
)
AND B.ACAD_PLAN_TYPE <> 'MIN'
UNION
SELECT DISTINCT A.ACAD_PROG, A.ACAD_PLAN
FROM PS_ACAD_PROG_TBL A
WHERE A.EFF_STATUS = 'A'
AND A.ACAD_CAREER = 'UGRD'
AND A.EFFDT = (
  SELECT MAX(A_ED.EFFDT)
  FROM PS_ACAD_PROG_TBL A_ED
  WHERE A.ACAD_PROG = A_ED.ACAD_PROG
)
AND A.ACAD_PLAN <> ' '
UNION
SELECT DISTINCT A.ACAD_PROG, B.ACAD_PLAN
FROM PS_ACAD_PROG_TBL A
INNER JOIN PS_ACAD_PLAN_TBL B ON A.ACAD_CAREER = B.ACAD_CAREER AND A.EFF_STATUS = B.EFF_STATUS AND B.EFFDT = (
  SELECT MAX(B_ED.EFFDT)
  FROM PS_ACAD_PLAN_TBL B_ED
  WHERE B.ACAD_PLAN = B_ED.ACAD_PLAN
)
WHERE A.EFF_STATUS = 'A'
AND A.ACAD_CAREER = 'UGRD'
AND A.EFFDT = (
  SELECT MAX(A_ED.EFFDT)
  FROM PS_ACAD_PROG_TBL A_ED
  WHERE A.ACAD_PROG = A_ED.ACAD_PROG
)
AND B.ACAD_PROG = ' '
AND B.ACAD_PLAN_TYPE <> 'MIN'
ORDER BY 1, 2"""

qi_orr2 = """SELECT A.PROG_ACTION, B.PROG_REASON
FROM PS_ADM_ACTION_TBL A
INNER JOIN PS_PROG_RSN_TBL B ON A.PROG_ACTION = B.PROG_ACTION AND A.EFF_STATUS = B.EFF_STATUS AND B.EFFDT = (
  SELECT MAX(B_ED.EFFDT)
  FROM PS_PROG_RSN_TBL B_ED
  WHERE B.PROG_ACTION = B_ED.PROG_ACTION
  AND B.PROG_REASON = B_ED.PROG_REASON
)
ORDER BY 1, 2"""

q0001 = """SELECT *
FROM mssbase as msb
INNER JOIN orabase as orb on msb.emplid = orb.emplid and msb.adm_appl_nbr = orb.adm_appl_nbr
WHERE NOT EXISTS (
  SELECT *
  FROM oraref1 as orr1
  WHERE msb.acad_prog = orr1.acad_prog
  AND msb.acad_plan = orr1.acad_plan
)
ORDER BY 1, 2"""

q0002 = """SELECT *
FROM mssbase as msb
INNER JOIN orabase as orb on msb.emplid = orb.emplid and msb.adm_appl_nbr = orb.adm_appl_nbr
WHERE NOT EXISTS (
  SELECT *
  FROM oraref2 as orr2
  WHERE msb.prog_action = orr2.prog_action
  AND msb.prog_reason = orr2.prog_reason
)
ORDER BY 1, 2"""

q0003 = """SELECT msb.*, orb.*
FROM mssbase as msb
INNER JOIN oraref3 as msborr3 on msb.prog_action = msborr3.prog_action
INNER JOIN orabase as orb on msb.emplid = orb.emplid and msb.adm_appl_nbr = orb.adm_appl_nbr
INNER JOIN oraref3 as orborr3 on orb.prog_action = orborr3.prog_action
WHERE msb.prog_action != orb.prog_action
AND msborr3.rank <= orborr3.rank
ORDER BY 1, 2"""

q0004 = """SELECT *
FROM mssbase as msb
INNER JOIN orabase as orb on msb.emplid = orb.emplid and msb.adm_appl_nbr = orb.adm_appl_nbr
INNER JOIN oraref3 as orr3 on orb.prog_action = orr3.prog_action
WHERE orr3.rank = 3
AND (
  (orb.admit_type != msb.admit_type
    AND orb.admit_type != 'XRE')
  OR orb.academic_level != msb.academic_level
  OR orb.admit_term != msb.admit_term
  OR orb.acad_prog != msb.acad_prog
  OR orb.acad_plan != msb.acad_plan
  OR orb.prog_action != msb.prog_action
  OR orb.prog_reason != msb.prog_reason
  OR orb.appl_fee_status != msb.appl_fee_status
)
ORDER BY 1, 2"""

# Referenced only as part of other queries in this module
q0005 = """
  SELECT umsb.adm_appl_nbr
  FROM mssbase as umsb
  INNER JOIN orabase as uorb on umsb.emplid = uorb.emplid and umsb.adm_appl_nbr = uorb.adm_appl_nbr
  WHERE NOT EXISTS (
    SELECT *
    FROM oraref1 as uorr1
    WHERE umsb.acad_prog = uorr1.acad_prog
    AND umsb.acad_plan = uorr1.acad_plan
  )
  UNION
  SELECT umsb.adm_appl_nbr
  FROM mssbase as umsb
  INNER JOIN orabase as uorb on umsb.emplid = uorb.emplid and umsb.adm_appl_nbr = uorb.adm_appl_nbr
  WHERE NOT EXISTS (
    SELECT *
    FROM oraref2 as uorr2
    WHERE umsb.prog_action = uorr2.prog_action
    AND umsb.prog_reason = uorr2.prog_reason
  )
  UNION
  SELECT umsb.adm_appl_nbr
  FROM mssbase as umsb
  INNER JOIN oraref3 as umsborr3 on umsb.prog_action = umsborr3.prog_action
  INNER JOIN orabase as uorb on umsb.emplid = uorb.emplid and umsb.adm_appl_nbr = uorb.adm_appl_nbr
  INNER JOIN oraref3 as uorborr3 on uorb.prog_action = uorborr3.prog_action
  WHERE umsb.prog_action != uorb.prog_action
  AND umsborr3.rank <= uorborr3.rank
  UNION
  SELECT umsb.adm_appl_nbr
  FROM mssbase as umsb
  INNER JOIN orabase as uorb on umsb.emplid = uorb.emplid and umsb.adm_appl_nbr = uorb.adm_appl_nbr
  INNER JOIN oraref3 as uorr3 on uorb.prog_action = uorr3.prog_action
  WHERE uorr3.rank = 3
  AND (
    (uorb.admit_type != umsb.admit_type
    AND uorb.admit_type != 'XRE')
    OR uorb.academic_level != umsb.academic_level
    OR uorb.admit_term != umsb.admit_term
    OR uorb.acad_prog != umsb.acad_prog
    OR uorb.acad_plan != umsb.acad_plan
    OR uorb.prog_action != umsb.prog_action
    OR uorb.prog_reason != umsb.prog_reason
    OR uorb.appl_fee_status != umsb.appl_fee_status
  )
"""

q0006 = """SELECT *
FROM mssbase as msb
INNER JOIN orabase as orb on msb.emplid = orb.emplid and msb.adm_appl_nbr = orb.adm_appl_nbr
WHERE msb.adm_appl_nbr NOT IN (""" + q0005 + """) AND msb.admit_type != orb.admit_type
AND orb.admit_type != 'XRE'
ORDER BY 1, 2"""

q0007 = """SELECT *
FROM mssbase as msb
INNER JOIN orabase as orb on msb.emplid = orb.emplid and msb.adm_appl_nbr = orb.adm_appl_nbr
WHERE msb.adm_appl_nbr NOT IN (""" + q0005 + """) AND msb.academic_level != orb.academic_level
ORDER BY 1, 2"""

q0008 = """SELECT *
FROM mssbase as msb
INNER JOIN orabase as orb on msb.emplid = orb.emplid and msb.adm_appl_nbr = orb.adm_appl_nbr
WHERE msb.adm_appl_nbr NOT IN (""" + q0005 + """) AND msb.admit_term != orb.admit_term
ORDER BY 1, 2"""

q0009 = """SELECT *
FROM mssbase as msb
INNER JOIN orabase as orb on msb.emplid = orb.emplid and msb.adm_appl_nbr = orb.adm_appl_nbr
WHERE msb.adm_appl_nbr NOT IN (""" + q0005 + """) AND msb.acad_prog != orb.acad_prog
ORDER BY 1, 2"""

q0010 = """SELECT *
FROM mssbase as msb
INNER JOIN orabase as orb on msb.emplid = orb.emplid and msb.adm_appl_nbr = orb.adm_appl_nbr
WHERE msb.adm_appl_nbr NOT IN (""" + q0005 + """) AND msb.acad_plan != orb.acad_plan
ORDER BY 1, 2"""

q0011 = """SELECT *
FROM mssbase as msb
INNER JOIN orabase as orb on msb.emplid = orb.emplid and msb.adm_appl_nbr = orb.adm_appl_nbr
WHERE msb.adm_appl_nbr NOT IN (""" + q0005 + """) AND msb.appl_fee_status != orb.appl_fee_status
ORDER BY 1, 2"""

q0012 = """SELECT
  msb.*,
  orb.*,
  '' as [BREAK],
  orx1.emplid,
  orx1.acad_career,
  orx1.stdnt_car_nbr,
  orx1.adm_appl_nbr,
  orx1.appl_prog_nbr,
  orx1.effdt,
  orx1.effseq,
  orx1.institution,
  orx1.acad_prog,
  orx1.prog_status,
  orx1.prog_action,
  orx1.action_dt,
  orx1.prog_reason,
  orx1.admit_term,
  orx1.exp_grad_term,
  orx1.req_term,
  orx1.acad_load_appr,
  orx1.campus,
  orx1.acad_prog_dual,
  orx1.joint_prog_appr,
  orx1.ssr_rs_candit_nbr,
  orx1.ssr_apt_instance,
  orx1.ssr_yr_of_prog,
  orx1.ssr_shift,
  orx1.ssr_cohort_id,
  orx1.scc_row_add_oprid,
  orx1.scc_row_add_dttm,
  orx1.scc_row_upd_oprid,
  orx1.scc_row_upd_dttm
FROM mssbase as msb
INNER JOIN orabase as orb on msb.emplid = orb.emplid and msb.adm_appl_nbr = orb.adm_appl_nbr
INNER JOIN oraaux1 as orx1 on orb.emplid = orx1.emplid and orb.adm_appl_nbr = orx1.adm_appl_nbr
WHERE msb.adm_appl_nbr NOT IN (""" + q0005 + """) AND msb.prog_action = orb.prog_action
AND msb.prog_reason != orb.prog_reason
ORDER BY 1, 2"""

q0013 = """SELECT
  msb.*,
  orb.*,
  '' as [BREAK],
  orx1.emplid,
  orx1.acad_career,
  orx1.stdnt_car_nbr,
  orx1.adm_appl_nbr,
  orx1.appl_prog_nbr,
  orx1.effdt,
  orx1.effseq,
  orx1.institution,
  msb.acad_prog,
  orr3.prog_status,
  msb.prog_action,
  '',
  msb.prog_reason,
  msb.admit_term,
  orx1.exp_grad_term,
  msb.admit_term,
  orx1.acad_load_appr,
  orx1.campus,
  orx1.acad_prog_dual,
  orx1.joint_prog_appr,
  orx1.ssr_rs_candit_nbr,
  orx1.ssr_apt_instance,
  orx1.ssr_yr_of_prog,
  orx1.ssr_shift,
  orx1.ssr_cohort_id,
  '' as [BREAK],
  orx1.Xemplid,
  orx1.Xacad_career,
  orx1.Xstdnt_car_nbr,
  orx1.Xadm_appl_nbr,
  orx1.Xappl_prog_nbr,
  orx1.Xeffdt,
  orx1.Xeffseq,
  msb.acad_plan,
  orx1.Xdeclare_dt,
  orx1.Xplan_sequence,
  msb.admit_term,
  orx1.Xssr_apt_instance,
  orx1.Xssr_yr_of_prog
FROM mssbase as msb
INNER JOIN oraref3 as orr3 on msb.prog_action = orr3.prog_action
INNER JOIN orabase as orb on msb.emplid = orb.emplid and msb.adm_appl_nbr = orb.adm_appl_nbr
INNER JOIN oraaux1 as orx1 on orb.emplid = orx1.emplid and orb.adm_appl_nbr = orx1.adm_appl_nbr
WHERE msb.adm_appl_nbr NOT IN (""" + q0005 + """) AND msb.prog_action != orb.prog_action
AND msb.admit_term is not null
AND msb.acad_prog is not null
AND msb.acad_plan is not null
ORDER BY 1, 2"""

q0014 = """SELECT *
FROM mssaux1 as msx1
INNER JOIN oraaux2 as orx2 on msx1.emplid = orx2.emplid
WHERE msx1.preferred is not null
ORDER BY 1"""

q0015 = """SELECT *
FROM mssaux1 as msx1
INNER JOIN oraaux2 as orx2 on msx1.emplid = orx2.emplid
WHERE msx1.mailing_street is not null
AND msx1.mailing_city is not null
ORDER BY 1"""

q0016 = """SELECT *
FROM mssaux1 as msx1
INNER JOIN oraaux2 as orx2 on msx1.emplid = orx2.emplid
WHERE msx1.permanent_street is not null
AND msx1.permanent_city is not null
ORDER BY 1"""

q0017 = """SELECT *
FROM mssaux1 as msx1
INNER JOIN oraaux2 as orx2 on msx1.emplid = orx2.emplid
WHERE msx1.primary_phone is not null
ORDER BY 1"""

q0018 = """SELECT *
FROM mssaux1 as msx1
INNER JOIN oraaux2 as orx2 on msx1.emplid = orx2.emplid
WHERE msx1.mobile_phone is not null
ORDER BY 1"""

q0019 = """SELECT *
FROM mssaux1 as msx1
INNER JOIN oraaux2 as orx2 on msx1.emplid = orx2.emplid
WHERE msx1.email is not null
ORDER BY 1"""

q0049 = """SELECT *
FROM mssaux1 as msx1
INNER JOIN oraaux2 as orx2 on msx1.emplid = orx2.emplid
INNER JOIN (
  SELECT
    msx2.emplid,
    group_concat(msx2.column) as illegal_columns
  FROM (
    SELECT *
    FROM mssaux2
    ORDER BY 1, 2
  ) as msx2
  GROUP BY emplid
  ORDER BY 1
) as msx2gc on msx1.emplid = msx2gc.emplid
ORDER BY 1"""

q0050 = """SELECT DISTINCT
  msx1.emplid,
  msxpn.preferred,
  msxpp.primary_phone,
  msxmp.mobile_phone,
  msxe.email,
  msxpa.permanent_country,
  msxpa.permanent_street,
  msxpa.permanent_city,
  msxpa.permanent_county,
  msxpa.permanent_region,
  msxpa.permanent_postal,
  msxma.mailing_country,
  msxma.mailing_street,
  msxma.mailing_city,
  msxma.mailing_county,
  msxma.mailing_region,
  msxma.mailing_postal,
  orx2.*
FROM mssaux1 as msx1
INNER JOIN oraaux2 as orx2 on msx1.emplid = orx2.emplid
LEFT OUTER JOIN mssaux1 as msxpn on (
  msx1.emplid = msxpn.emplid
  AND msxpn.preferred is not null
  AND msxpn.preferred != coalesce(orx2.preferred_name, '')
)
LEFT OUTER JOIN mssaux1 as msxpp on (
  msx1.emplid = msxpp.emplid
  AND msxpp.primary_phone is not null
  AND msxpp.primary_phone != coalesce(orx2.home_phone, '')
)
LEFT OUTER JOIN mssaux1 as msxmp on (
  msx1.emplid = msxmp.emplid
  AND msxmp.mobile_phone is not null
  AND msxmp.mobile_phone != coalesce(orx2.cell_phone, '')
)
LEFT OUTER JOIN mssaux1 as msxe on (
  msx1.emplid = msxe.emplid
  AND msxe.email is not null
  AND msxe.email != coalesce(orx2.other_email, '')
)
LEFT OUTER JOIN mssaux1 as msxpa on (
  msx1.emplid = msxpa.emplid
  AND NOT EXISTS (
    SELECT *
    FROM mssaux3 as msx3
    WHERE msxpa.emplid = msx3.emplid
  ) AND msxpa.permanent_street is not null
  AND msxpa.permanent_city is not null
  AND (
    msxpa.permanent_country != coalesce(orx2.home_country, '')
    OR msxpa.permanent_street != coalesce(orx2.home_street, '')
    OR msxpa.permanent_city != coalesce(orx2.home_city, '')
    OR msxpa.permanent_county != coalesce(orx2.home_county, '')
    OR msxpa.permanent_region != coalesce(orx2.home_state, '')
    OR msxpa.permanent_postal != coalesce(orx2.home_postal, '')
  )
)
LEFT OUTER JOIN mssaux1 as msxma on (
  msx1.emplid = msxma.emplid
  AND msxma.mailing_street is not null
  AND msxma.mailing_city is not null
  AND (
    msxma.mailing_country != coalesce(orx2.mail_country, '')
    OR msxma.mailing_street != coalesce(orx2.mail_street, '')
    OR msxma.mailing_city != coalesce(orx2.mail_city, '')
    OR msxma.mailing_county != coalesce(orx2.mail_county, '')
    OR msxma.mailing_region != coalesce(orx2.mail_state, '')
    OR msxma.mailing_postal != coalesce(orx2.mail_postal, '')
  )
)
WHERE NOT EXISTS (
  SELECT *
  FROM mssaux2 as msx2
  WHERE msx1.emplid = msx2.emplid
) AND (
  msxpn.emplid is not null
  OR msxpp.emplid is not null
  OR msxmp.emplid is not null
  OR msxe.emplid is not null
  OR msxpa.emplid is not null
  OR msxma.emplid is not null
)
ORDER BY 1"""

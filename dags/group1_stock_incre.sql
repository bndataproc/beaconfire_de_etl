merge into BEACONFIRE.DEV_DB.group1_dim_COMPANY_PROFILE as T
using BEACONFIRE.DEV_DB.group1_COMPANY_PROFILE_prestage as S
on T.symbol=S.symbol
when not matched then
insert (symbol,price,beta,volavg,mktcap
       ,lastdiv,range,changes,companyname
       ,exchange,industry,sector,dcfdiff,dcf) values (S.symbol,S.price,S.beta,S.volavg,S.mktcap
                                                      ,S.lastdiv,S.range,S.changes,S.companyname
                                                      ,S.exchange,S.industry,S.sector,S.dcfdiff,S.dcf)
when matched then
update set T.symbol=S.symbol,
           T.price=S.price,
           T.beta=S.beta,
           T.volavg=S.volavg,
           T.mktcap=S.mktcap,
           T.lastdiv=T.lastdiv,
           T.range=S.range,
           T.changes=S.changes,
           T.companyname=S.companyname,
           T.exchange=S.exchange,
           T.industry=S.industry,
           T.sector=S.sector,
           T.dcfdiff=S.dcfdiff,
           T.dcf=S.dcf
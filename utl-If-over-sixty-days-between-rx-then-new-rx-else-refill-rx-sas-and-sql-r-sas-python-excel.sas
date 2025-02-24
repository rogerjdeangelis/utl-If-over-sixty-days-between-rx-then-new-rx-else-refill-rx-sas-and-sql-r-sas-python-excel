%let pgm=utl-If-over-sixty-days-between-rx-then-new-rx-else-refill-rx-sas-and-sql-r-sas-python-excel;

%stop_submission;

If over sixty days between Rx then  new rx else refill rx sas and sql r sas python excel

   SOLUTIONS

      1 sas sql  (no sort needed - more complicated than sqllite, because proc sql does not hae a lag function)
        (may be able ro eliminate the sort and simplify using my sqlpartitionx macro)
      2 sas base (requires pre sorting)
        ksharp
        https://communities.sas.com/t5/user/viewprofilepage/user-id/18408
      3 r sql (no sort needed)
        (r sql supports the lag function, so we have a simpler solution)
      4 python sql (same exact code as r)
      5 excel sql (same exact code as r)

github
https://tinyurl.com/4vasvxkd
https://github.com/rogerjdeangelis/utl-If-over-sixty-days-between-rx-then-new-rx-else-refill-rx-sas-and-sql-r-sas-python-excel

sas communities
https://tinyurl.com/yu2mz2z3
https://communities.sas.com/t5/SAS-Programming/Creating-new-variables-based-on-most-recent-date/m-p/959715#M374416

SOAPBOX ON
  As a side note, I prefer iso character dates (e8601da.) because
  they sort in the correct order, unlike date9 format.
  Also many SQL dialects can work directly with iso character dates.
  Also you can use max and min with iso dates,
  I combined the grouping variable to simplify the code.
  This does not matter.

Instead of two colums, REFILL and NEW, I create one column RX
This is a better data structure

                 OPTIONAL COLUMN
        RX          RX_CODE

      FIRST RX        1
      NEW RX          2
      REFILL RX       3

 NOTE RX is exclusive

SOAPBOX OFF

/*               _     _
 _ __  _ __ ___ | |__ | | ___ _ __ ___
| `_ \| `__/ _ \| `_ \| |/ _ \ `_ ` _ \
| |_) | | | (_) | |_) | |  __/ | | | | |
| .__/|_|  \___/|_.__/|_|\___|_| |_| |_|
|_|
*/

/**************************************************************************************************************************/
/*                         |                                                       |                                      */
/*       INPUT             |        PROCESS                                        |         OUTPUT                       */
/*       =====             |        =======                                        |         ======                       */
/*                         |If over 60 days between Rx then new rx else refill rx  |                                      */
/*                         |                                                       |                                      */
/*                         |                                                       |                                      */
/*                         | select cur date - prev date from                      | GRP    DATE    DAYS    RX            */
/* GRP       DATE          | self join of have                                     |                                      */
/*                         | on cur grp = pre grp                                  | 1aa 2020-01-01    .    FIRST RX      */
/* 2aa    2020-02-01       | and cur.date > pre.date                               | 1aa 2020-04-04   94    NEW RX        */
/* 2aa    2020-07-07       | * the following is key because  it                    | 1aa 2020-04-21   17    REFILL RX     */
/* 2ab    2020-07-01       | limits comparisons to consecutive pairs               | 1aa 2020-07-01   71    NEW RX        */
/* 3ba    2020-04-04       | group by cur.grp, cur.date                            | 2aa 2020-02-01    .    FIRST RX      */
/* 3ba    2020-05-01       | having pre.date = max(pre.date)                       | 2aa 2020-07-07  157    NEW RX        */
/* 3ba    2020-06-15       | or pre.date is null                                   | 2ab 2020-07-01    .    FIRST RX      */
/* 3ba    2020-09-01       |                                                       | 3ba 2020-04-04    .    FIRST RX      */
/* 3bb    2020-01-01       | 1 SAS SQL                                             | 3ba 2020-05-01   27    REFILL RX     */
/* 3bb    2020-09-02       | =========                                             | 3ba 2020-06-15   45    REFILL RX     */
/* 1aa    2020-01-01       | Proc datasets  nodetails nolist;delete want;run;      | 3ba 2020-09-01   78    NEW RX        */
/* 1aa    2020-04-04       |                                                       | 3bb 2020-01-01    .    FIRST RX      */
/* 1aa    2020-04-21       | proc sql;                                             | 3bb 2020-09-02  245    NEW RX        */
/* 1aa    2020-07-01       |  create table want as                                 |                                      */
/*                         |  select                                               |                                      */
/*                         |    cur.grp                                            |                                      */
/* options                 |   ,cur.date                                           |                                      */
/*  validvarname=upcase;   |   ,input(cur.date,e8601da.) -                         |                                      */
/* libname sd1 "d:/sd1";   |      input(pre.date,e8601da.) as days                 |                                      */
/* data sd1.have;          |   ,case                                               |                                      */
/*   input grp$ date $10.; |      when (calculated days le  0) then "FIRST RX "    |                                      */
/* cards4;                 |      when (calculated days ge 60) then "NEW RX   "    |                                      */
/* 2aa 2020-02-01          |      when (calculated days lt 60) then "REFILL RX"    |                                      */
/* 2aa 2020-07-07          |      else "ERROR"                                     |                                      */
/* 2ab 2020-07-01          |    end as rx                                          |                                      */
/* 3ba 2020-04-04          |  from                                                 |                                      */
/* 3ba 2020-05-01          |    sd1.have cur left join sd1.have pre                |                                      */
/* 3ba 2020-06-15          |  on                                                   |                                      */
/* 3ba 2020-09-01          |        cur.grp = pre.grp                              |                                      */
/* 3bb 2020-01-01          |    and cur.date > pre.date                            |                                      */
/* 3bb 2020-09-02          |  group                                                |                                      */
/* 1aa 2020-01-01          |    by cur.grp, cur.date                               |                                      */
/* 1aa 2020-04-04          |  having                                               |                                      */
/* 1aa 2020-04-21          |       pre.date = max(pre.date)                        |                                      */
/* 1aa 2020-07-01          |    or pre.date is null                                |                                      */
/* ;;;;                    |  order                                                |                                      */
/* run;quit;               |    by cur.grp, cur.date;                              |                                      */
/*                         | quit;                                                 |                                      */
/*                         |                                                       |                                      */
/*                         |----------------------------------------------------------------------------------------------*/
/*                         |                                                       |                                      */
/*                         | 2 BASE SAS                                            |GRP       DATE       RX               */
/*                         | ==========                                            |                                      */
/*                         |                                                       |1aa    2020-01-01    FIRST RX         */
/*                         | * it requires a sort;                                 |1aa    2020-04-04    NEW RX           */
/*                         |                                                       |1aa    2020-04-21    REFILL RX        */
/*                         | proc sort data=sd1.have out=havsrt;                   |1aa    2020-07-01    NEW RX           */
/*                         |  by grp date;                                         |2aa    2020-02-01    FIRST RX         */
/*                         | run;quit;                                             |2aa    2020-07-07    NEW RX           */
/*                         |                                                       |2ab    2020-07-01    FIRST RX         */
/*                         | data want;                                            |3ba    2020-04-04    FIRST RX         */
/*                         |  set havsrt;                                          |3ba    2020-05-01    REFILL RX        */
/*                         |  by grp;                                              |3ba    2020-06-15    REFILL RX        */
/*                         |  daten=input(date,e8601da.);                          |3ba    2020-09-01    NEW RX           */
/*                         |  difstart=dif(daten);                                 |3bb    2020-01-01    FIRST RX         */
/*                         |  if not first.grp and                                 |3bb    2020-09-02    NEW RX           */
/*                         |    .<dif(daten)<60 then rx="REFILL RX";               |                                      */
/*                         |  if not first.grp and                                 |                                      */
/*                         |     dif(daten)>60  then rx="NEW RX";                  |                                      */
/*                         |  if difstart < 0 then rx="FIRST RX";                  |                                      */
/*                         |  drop difstart daten;                                 |                                      */
/*                         | run;quit;                                             |                                      */
/*                         |                                                       |                                      */
/* -----------------------------------------------------------------------------------------------------------------------*/
/*                         |                                                       |                                      */
/*                         | 3 R SQL                                               | SAS                                  */
/*                         | =======                                               |                                      */
/*                         |                                                       |  GRP    DATE    DAYS    RX           */
/*                         | proc datasets lib=sd1 nolist nodetails;               |                                      */
/*                         |  delete want;                                         |  1aa 2020-01-01    .    FIRST RX     */
/*                         | run;quit;                                             |  1aa 2020-04-04   94    NEW RX       */
/*                         |                                                       |  1aa 2020-04-21   17    REFILL RX    */
/*                         | %utl_rbeginx;                                         |  1aa 2020-07-01   71    NEW RX       */
/*                         | parmcards4;                                           |  2aa 2020-02-01    .    FIRST RX     */
/*                         | library(haven)                                        |  2aa 2020-07-07  157    NEW RX       */
/*                         | library(sqldf)                                        |  2ab 2020-07-01    .    FIRST RX     */
/*                         | source("c:/oto/fn_tosas9x.R")                         |  3ba 2020-04-04    .    FIRST RX     */
/*                         | have<-read_sas("d:/sd1/have.sas7bdat")                |  3ba 2020-05-01   27    REFILL RX    */
/*                         | want<-sqldf('                                         |  3ba 2020-06-15   45    REFILL RX    */
/*                         | select                                                |  3ba 2020-09-01   78    NEW RX       */
/*                         |   grp,                                                |  3bb 2020-01-01    .    FIRST RX     */
/*                         |   date,                                               |                                      */
/*                         |   julianday(date)-julianday(lag(date) over            | R                                    */
/*                         |    (partition by grp order by date)) as days          |                                      */
/*                         | from have                                             | grp       date days        rx        */
/*                         | order by grp, date                                    |                                      */
/*                         | ')                                                    | 1aa 2020-01-01   NA  FIRST RX        */
/*                         | want                                                  | 1aa 2020-04-04   94 NEW RX           */
/*                         | fn_tosas9x(                                           | 1aa 2020-04-21   17 REFILL RX        */
/*                         |       inp    = want                                   | 1aa 2020-07-01   71 NEW RX           */
/*                         |      ,outlib ="d:/sd1/"                               | 2aa 2020-02-01   NA  FIRST RX        */
/*                         |      ,outdsn ="want"                                  | 2aa 2020-07-07  157 NEW RX           */
/*                         |      )                                                | 2ab 2020-07-01   NA  FIRST RX        */
/*                         | ;;;;                                                  | 3ba 2020-04-04   NA  FIRST RX        */
/*                         | %utl_rendx;                                           | 3ba 2020-05-01   27 REFILL RX        */
/*                         |                                                       | 3ba 2020-06-15   45 REFILL RX        */
/*                         | proc print data=sd1.want(drop=rpwnames);              | 3ba 2020-09-01   78 NEW RX           */
/*                         |                                                       | 3bb 2020-01-01   NA  FIRST RX        */
/*                         | run;quit;                                             | 3bb 2020-09-02  245 NEW RX           */
/*                         |                                                       |                                      */
/*                         |----------------------------------------------------------------------------------------------*/
/*                         |                                                       |                                      */
/*                         | 4 PYTHON SQL                                          |                                      */
/*                         | ============                                          |  Python                              */
/*                         |                                                       |      grp     date   days        rx   */
/*                         | proc datasets lib=sd1 nolist nodetails;               |  0  1aa 2020-01-01   NaN   FIRST RX  */
/*                         |  delete pywant;                                       |  1  1aa 2020-04-04  94.0  NEW RX     */
/*                         | run;quit;                                             |  2  1aa 2020-04-21  17.0  REFILL RX  */
/*                         |                                                       |  3  1aa 2020-07-01  71.0  NEW RX     */
/*                         | %utl_pybeginx;                                        |  4  2aa 2020-02-01   NaN   FIRST RX  */
/*                         | parmcards4;                                           |  5  2aa 2020-07-07 157.0  NEW RX     */
/*                         | exec(open('c:/oto/fn_python.py').read());             |  6  2ab 2020-07-01   NaN   FIRST RX  */
/*                         | have,meta = ps.read_sas7bdat('d:/sd1/have.sas7bdat'); |  7  3ba 2020-04-04   NaN   FIRST RX  */
/*                         | want = pdsql('''                                      |  8  3ba 2020-05-01  27.0  REFILL RX  */
/*                         | select                                        \       |  9  3ba 2020-06-15  45.0  REFILL RX  */
/*                         |   grp,                                        \       |  10 3ba 2020-09-01  78.0  NEW RX     */
/*                         |   date,                                       \       |  11 3bb 2020-01-01   NaN   FIRST RX  */
/*                         |   julianday(date)-julianday(lag(date) over    \       |  12 3bb 2020-09-02 245.0  NEW RX     */
/*                         |    (partition by grp order by date)) as days  \       |                                      */
/*                         | from have                                     \       | SAS                                  */
/*                         | order by grp, date                            \       |  GRP    DATE    DAYS    RX           */
/*                         | ''')                                                  |                                      */
/*                         | print(want)                                           |  1aa 2020-01-01    .    FIRST RX     */
/*                         | fn_tosas9x(want,outlib='d:/sd1/',outdsn='pywant');    |  1aa 2020-04-04   94    NEW RX       */
/*                         | ;;;;                                                  |  1aa 2020-04-21   17    REFILL RX    */
/*                         | %utl_pyendx;                                          |  1aa 2020-07-01   71    NEW RX       */
/*                         |                                                       |  2aa 2020-02-01    .    FIRST RX     */
/*                         | proc print data=sd1.pywant;                           |  2aa 2020-07-07  157    NEW RX       */
/*                         | run;quit;                                             |  2ab 2020-07-01    .    FIRST RX     */
/*                         |                                                       |  ...                                 */
/*                         |                                                       |                                      */
/*                         |----------------------------------------------------------------------------------------------*/
/*                         |                                                       |                                      */
/*                         | 5 EXCEL SQL                                           |                                      */
/*                         | ===========                                           |d:/xls/wantxl.xlsx                    */
/*                         |                                                       |                                      */
/*                         | proc datasets lib=sd1 nolist nodetails;               |-------------------+                  */
/*                         |  delete want;                                         || A1|fx  |GRP       |                 */
/*                         | run;quit;                                             |------------------------------------+ */
/*                         |                                                       |[_] |  A |    B      | C  |    D    | */
/*                         | %utlfkil(d:/xls/wantxl.xlsx);                         |------------------------------------| */
/*                         |                                                       | 1  | GRP|  DATE     |DAYS|   RX    | */
/*                         | * create input sheet;                                 | -- |----+-----------+----+---------| */
/*                         | %utl_rbeginx;                                         | 2  |1aa | 2020-01-01| .  | FIRST RX| */
/*                         | parmcards4;                                           | -- |----+-----------+----+---------| */
/*                         | library(openxlsx)                                     | 3  |1aa | 2020-04-04| 94 | NEW RX  | */
/*                         | library(sqldf)                                        | -- |----+-----------+----+---------| */
/*                         | library(haven)                                        | 4  |1aa | 2020-04-21| 17 | REFILL R| */
/*                         | have<-read_sas("d:/sd1/have.sas7bdat")                | -- |----+-----------+----+---------| */
/*                         | wb <- createWorkbook()                                | 5  |1aa | 2020-07-01| 71 | NEW RX  | */
/*                         | addWorksheet(wb, "have")                              | -- |----+-----------+----+---------| */
/*                         | writeData(wb, sheet = "have", x = have)               | 6  |2aa | 2020-02-01| .  | FIRST RX| */
/*                         | saveWorkbook(                                         | -- |----+-----------+----+---------| */
/*                         |     wb                                                | 7  |2aa | 2020-07-07| 157| NEW RX  | */
/*                         |    ,"d:/xls/wantxl.xlsx"                              | -- |----+-----------+----+---------| */
/*                         |    ,overwrite=TRUE)                                   | 8  |2ab | 2020-07-01| .  | FIRST RX| */
/*                         | ;;;;                                                  | -- |----+-----------+----+---------| */
/*                         | %utl_rendx;                                           | 9  |3ba | 2020-04-04| .  | FIRST RX| */
/*                         |                                                       | -- |----+-----------+----+---------| */
/*                         | %utl_rbeginx;                                         |10  |3ba | 2020-05-01| 27 | REFILL R| */
/*                         | parmcards4;                                           | -- |----+-----------+----+---------| */
/*                         | library(openxlsx)                                     |11  |3ba | 2020-06-15| 45 | REFILL R| */
/*                         | library(sqldf)                                        | -- |----+-----------+----+---------| */
/*                         | source("c:/oto/fn_tosas9x.R")                         |12  |3ba | 2020-09-01| 78 | NEW RX  | */
/*                         |  wb<-loadWorkbook("d:/xls/wantxl.xlsx")               | -- |----+-----------+----+---------| */
/*                         |  have<-read.xlsx(wb,"have")                           |13  |3bb | 2020-01-01| .  | FIRST RX| */
/*                         |  addWorksheet(wb, "want")                             | -- |----+-----------+----+---------| */
/*                         |   want<-sqldf('                                       |14  |3bb | 2020-09-02| 245| NEW RX  | */
/*                         |   select                                              | -- |----+--------------------------+ */
/*                         |     grp,                                              |[WANT}                                */
/*                         |     date,                                             |                                      */
/*                         |     julianday(date)-julianday(lag(date) over          |                                      */
/*                         |      (partition by grp order by date)) as days        |                                      */
/*                         |   from have                                           |                                      */
/*                         |   order by grp, date                                  |                                      */
/*                         |   ')                                                  |                                      */
/*                         | print(want)                                           |                                      */
/*                         | writeData(wb,sheet="want",x=want)                     |                                      */
/*                         | saveWorkbook(                                         |                                      */
/*                         |     wb                                                |                                      */
/*                         |    ,"d:/xls/wantxl.xlsx"                              |                                      */
/*                         |    ,overwrite=TRUE)                                   |                                      */
/*                         | fn_tosas9x(                                           |                                      */
/*                         |       inp    = want                                   |                                      */
/*                         |      ,outlib ="d:/sd1/"                               |                                      */
/*                         |      ,outdsn ="want"                                  |                                      */
/*                         |      )                                                |                                      */
/*                         | ;;;;                                                  |                                      */
/*                         | %utl_rendx;                                           |                                      */
/*                         |                                                       |                                      */
/*                         | proc print data=sd1.want;                             |                                      */
/*                         | run;quit;                                             |                                      */
/*                         |                                                       |                                      */
/**************************************************************************************************************************/

/*              _
  ___ _ __   __| |
 / _ \ `_ \ / _` |
|  __/ | | | (_| |
 \___|_| |_|\__,_|

*/

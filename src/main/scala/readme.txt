//for 1 hour bars, 1h = 3600 sec.
CREATE TABLE mts_bars.td_bars_3600(
  ticker_id int,
	ddate     date,
  bar_1 map<text,text>,
  bar_2 map<text,text>,
  bar_3 map<text,text>,
  bar_4 map<text,text>,
  bar_5 map<text,text>,
  bar_6 map<text,text>,
  bar_7 map<text,text>,
  bar_8 map<text,text>,
  bar_9 map<text,text>,
  bar_10 map<text,text>,
  bar_11 map<text,text>,
  bar_12 map<text,text>,
  bar_13 map<text,text>,
  bar_14 map<text,text>,
  bar_15 map<text,text>,
  bar_16 map<text,text>,
  bar_17 map<text,text>,
  bar_18 map<text,text>,
  bar_19 map<text,text>,
  bar_20 map<text,text>,
  bar_21 map<text,text>,
  bar_22 map<text,text>,
  bar_23 map<text,text>,
  bar_24 map<text,text>,
	PRIMARY KEY ((ticker_id), ddate)
) WITH CLUSTERING ORDER BY (ddate DESC)

select '  bar_'||rownum||' map<text,text>,'
 from dual
 connect by rownum<=24


truncate mts_bars.td_bars_3600;

select * from mts_bars.td_bars_3600;

insert into mts_bars.td_bars_3600(ticker_id,ddate,bar_1) values(1,'2019-01-25',
{
  'bar_width_sec' : '3659',
  'btype' : 'd',
  'c' : '1.67173',
  'disp' : '0.0017',
  'h' : '1.67184',
  'h_body' : '0.0013',
  'h_shad' : '0.0011',
  'l' : '1.67141',
  'log_co' : '0.023',
  'o' : '1.67156',
  'ticks_cnt' : '35746',
  'ts_begin' : '1548839178248',
  'ts_end' : '1548839188248'
});

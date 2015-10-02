import java.io.IOException;

import org.apache.tajo.client.v2.TajoClient;
import org.apache.tajo.client.v2.exception.ClientUnableToConnectException;
import org.apache.tajo.exception.QueryFailedException;
import org.apache.tajo.exception.QueryKilledException;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.exception.UndefinedDatabaseException;
import org.apache.tajo.util.Pair;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

public class TajoTPCH {
    String[] Query;

    public TajoTPCH() {
        Query = new String[22];
        Query[0] = "select " +
                "l_returnflag, " +
                "l_linestatus, " +
                "sum(l_quantity) as sum_qty, " +
                "sum(l_extendedprice) as sum_base_price, " +
                "sum(l_extendedprice*(1-l_discount)) as sum_disc_price, " +
                "sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge, " +
                "avg(l_quantity) as avg_qty, " +
                "avg(l_extendedprice) as avg_price, " +
                "avg(l_discount) as avg_disc, " +
                "count(*) as count_order " +
                "from " +
                "lineitem " +
                "where " +
                "l_shipdate <= date '1998-09-01' " +
                "group by " +
                "l_returnflag, " +
                "l_linestatus " +
                "order by " +
                "l_returnflag, " +
                "l_linestatus; ";
        Query[1] = "DROP TABLE IF EXISTS q2_minimum_cost_supplier_tmp1; " +
                "DROP TABLE IF EXISTS q2_minimum_cost_supplier_tmp2; " +
                " " +
                "create table q2_minimum_cost_supplier_tmp1 (s_acctbal double, s_name text, n_name text, p_partkey int8," +
                " ps_supplycost double, p_mfgr text, s_address text, s_phone text, s_comment text); " +
                "create table q2_minimum_cost_supplier_tmp2 (p_partkey int8, ps_min_supplycost double); " +
                " " +
                "insert overwrite into q2_minimum_cost_supplier_tmp1  " +
                "select  " +
                "  s.s_acctbal, s.s_name, n.n_name, p.p_partkey, ps.ps_supplycost, p.p_mfgr, s.s_address, s.s_phone, s.s_comment  " +
                "from  " +
                "  nation n join region r  " +
                "  on  " +
                "    n.n_regionkey = r.r_regionkey and r.r_name = 'EUROPE'  " +
                "  join supplier s  " +
                "  on  " +
                "s.s_nationkey = n.n_nationkey  " +
                "  join partsupp ps  " +
                "  on   " +
                "s.s_suppkey = ps.ps_suppkey  " +
                "  join part p  " +
                "  on  " +
                "    p.p_partkey = ps.ps_partkey and p.p_size = 15 and p.p_type like '%BRASS' ; " +
                " " +
                "insert overwrite into q2_minimum_cost_supplier_tmp2  " +
                "select  " +
                "  p_partkey, min(ps_supplycost)  " +
                "from   " +
                "  q2_minimum_cost_supplier_tmp1  " +
                "group by p_partkey; " +
                " " +
                "select  " +
                "  t1.s_acctbal, t1.s_name, t1.n_name, t1.p_partkey, t1.p_mfgr, t1.s_address, t1.s_phone, t1.s_comment  " +
                "from  " +
                "  q2_minimum_cost_supplier_tmp1 t1 join q2_minimum_cost_supplier_tmp2 t2  " +
                "on  " +
                "  t1.p_partkey = t2.p_partkey and t1.ps_supplycost=t2.ps_min_supplycost  " +
                "order by s_acctbal desc, n_name, s_name, t1.p_partkey  " +
                "limit 100; " +
                " ";
        Query[2] = "select " +
                "l_orderkey, " +
                "sum(l_extendedprice*(1-l_discount)) as revenue, " +
                "o_orderdate, " +
                "o_shippriority " +
                "from " +
                "customer, " +
                "orders, " +
                "lineitem " +
                "where " +
                "c_mktsegment = 'BUILDING' " +
                "and c_custkey = o_custkey " +
                "and l_orderkey = o_orderkey " +
                "and o_orderdate < date '1995-03-15' " +
                "and l_shipdate > date '1995-03-15' " +
                "group by " +
                "l_orderkey, " +
                "o_orderdate, " +
                "o_shippriority " +
                "order by " +
                "revenue desc, " +
                "o_orderdate " +
                "limit 10; ";
        Query[3] = "DROP TABLE IF EXISTS q4_order_priority_tmp; " +
                " " +
                "CREATE TABLE q4_order_priority_tmp (O_ORDERKEY INT8); " +
                " " +
                "INSERT OVERWRITE INTO q4_order_priority_tmp  " +
                "select  " +
                " DISTINCT l_orderkey  " +
                "from  " +
                " lineitem " +
                "where  " +
                " l_commitdate < l_receiptdate; " +
                " " +
                " " +
                "select " +
                " o_orderpriority, " +
                " count(*) as order_count  " +
                "from  " +
                " orders o  " +
                " join " +
                "  q4_order_priority_tmp t  " +
                " on " +
                "  o.o_orderkey = t.o_orderkey " +
                "  and o.o_orderdate >= '1993-07-01'::date " +
                "  and o.o_orderdate < '1993-10-01'::date " +
                "group by " +
                " o_orderpriority  " +
                "order by " +
                " o_orderpriority; ";
        Query[4] = "select " +
                " n_name, " +
                " sum(l_extendedprice * (1 - l_discount)) as revenue " +
                "from " +
                " customer, " +
                " orders, " +
                " lineitem, " +
                " supplier, " +
                " nation, " +
                " region " +
                "where " +
                " c_custkey = o_custkey " +
                " and l_orderkey = o_orderkey " +
                " and l_suppkey = s_suppkey " +
                " and c_nationkey = s_nationkey " +
                " and s_nationkey = n_nationkey " +
                " and n_regionkey = r_regionkey " +
                " and r_name = 'ASIA' " +
                " and o_orderdate >= '1994-01-01'::date " +
                " and o_orderdate < '1995-01-01'::timestamp " +
                "group by " +
                " n_name " +
                "order by " +
                " revenue desc; ";
        Query[5] = "select " +
                "sum(l_extendedprice*l_discount) as revenue " +
                "from " +
                "lineitem " +
                "where " +
                "l_shipdate >= date '1994-01-01' " +
                "and l_shipdate < date '1995-01-01' " +
                "and l_discount between 0.05 and 0.07 " +
                "and l_quantity < 24; ";
        Query[6] = "select " +
                "supp_nation, " +
                "cust_nation, " +
                "l_year, sum(volume) as revenue " +
                "from ( " +
                "select " +
                "n1.n_name as supp_nation, " +
                "n2.n_name as cust_nation, " +
                "extract(year from l_shipdate) as l_year, " +
                "l_extendedprice * (1 - l_discount) as volume " +
                "from " +
                "supplier, " +
                "lineitem, " +
                "orders, " +
                "customer, " +
                "nation n1, " +
                "nation n2 " +
                "where " +
                "s_suppkey = l_suppkey " +
                "and o_orderkey = l_orderkey " +
                "and c_custkey = o_custkey " +
                "and s_nationkey = n1.n_nationkey " +
                "and c_nationkey = n2.n_nationkey " +
                "and ( " +
                "(n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY') " +
                "or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE') " +
                ") " +
                "and l_shipdate between date '1995-01-01' and date '1996-12-31' " +
                ") as shipping " +
                "group by " +
                "supp_nation, " +
                "cust_nation, " +
                "l_year " +
                "order by " +
                "supp_nation, " +
                "cust_nation, " +
                "l_year; ";
        Query[7] = "select " +
                "o_year, " +
                "sum(case " +
                "when nation = 'BRAZIL' " +
                "then volume " +
                "else 0 " +
                "end) / sum(volume) as mkt_share " +
                "from ( " +
                "select " +
                "extract(year from o_orderdate) as o_year, " +
                "l_extendedprice * (1-l_discount) as volume, " +
                "n2.n_name as nation " +
                "from " +
                "part, " +
                "supplier, " +
                "lineitem, " +
                "orders, " +
                "customer, " +
                "nation n1, " +
                "nation n2, " +
                "region " +
                "where " +
                "p_partkey = l_partkey " +
                "and s_suppkey = l_suppkey " +
                "and l_orderkey = o_orderkey " +
                "and o_custkey = c_custkey " +
                "and c_nationkey = n1.n_nationkey " +
                "and n1.n_regionkey = r_regionkey " +
                "and r_name = 'AMERICA' " +
                "and s_nationkey = n2.n_nationkey " +
                "and o_orderdate between date '1995-01-01' and date '1996-12-31' " +
                "and p_type = 'ECONOMY ANODIZED STEEL' " +
                ") as all_nations " +
                "group by " +
                "o_year " +
                "order by " +
                "o_year; ";
        Query[8] = "select " +
                "nation, " +
                "o_year, " +
                "sum(amount) as sum_profit " +
                "from ( " +
                "select " +
                "n_name as nation, " +
                "extract(year from o_orderdate) as o_year, " +
                "l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount " +
                "from " +
                "part, " +
                "supplier, " +
                "lineitem, " +
                "partsupp, " +
                "orders, " +
                "nation " +
                "where " +
                "s_suppkey = l_suppkey " +
                "and ps_suppkey = l_suppkey " +
                "and ps_partkey = l_partkey " +
                "and p_partkey = l_partkey " +
                "and o_orderkey = l_orderkey " +
                "and s_nationkey = n_nationkey " +
                "and p_name like '%green%' " +
                ") as profit " +
                "group by " +
                "nation, " +
                "o_year " +
                "order by " +
                "nation, " +
                "o_year desc; ";
        Query[9] = "select  " +
                " c_custkey, " +
                " c_name, " +
                " sum(l_extendedprice * (1 - l_discount)) as revenue,  " +
                " c_acctbal, n_name, c_address, c_phone, c_comment " +
                "from " +
                " customer c " +
                "join " +
                " orders o  " +
                "on  " +
                " c.c_custkey = o.o_custkey " +
                " and o.o_orderdate >= '1993-10-01'::date " +
                " and o.o_orderdate < '1994-01-01'::date " +
                "join nation n  " +
                "on  " +
                " c.c_nationkey = n.n_nationkey " +
                "join " +
                " lineitem l  " +
                "on  " +
                " l.l_orderkey = o.o_orderkey " +
                " and l.l_returnflag = 'R' " +
                "group by " +
                " c_custkey, " +
                " c_name, " +
                " c_acctbal, " +
                " c_phone, " +
                " n_name, " +
                " c_address, " +
                " c_comment  " +
                "order by " +
                " revenue desc  " +
                "limit 20; ";
        Query[10] = "select " +
                "ps_partkey, " +
                "sum(ps_supplycost * ps_availqty) as value " +
                "from " +
                "partsupp, " +
                "supplier, " +
                "nation " +
                "where " +
                "ps_suppkey = s_suppkey " +
                "and s_nationkey = n_nationkey " +
                "and n_name = 'GERMANY' " +
                "group by " +
                "ps_partkey having " +
                "sum(ps_supplycost * ps_availqty) > 7874103.10 " +
                "order by " +
                "value desc; ";
        Query[11] = "select " +
                "l_shipmode, " +
                "sum(case " +
                "when o_orderpriority ='1-URGENT' " +
                "or o_orderpriority ='2-HIGH' " +
                "then 1 " +
                "else 0 " +
                "end) as high_line_count, " +
                "sum(case " +
                "when o_orderpriority <> '1-URGENT' " +
                "and o_orderpriority <> '2-HIGH' " +
                "then 1 " +
                "else 0 " +
                "end) as low_line_count " +
                "from " +
                "orders, " +
                "lineitem " +
                "where " +
                "o_orderkey = l_orderkey " +
                "and l_shipmode in ('MAIL', 'SHIP') " +
                "and l_commitdate < l_receiptdate " +
                "and l_shipdate < l_commitdate " +
                "and l_receiptdate >= date '1994-01-01' " +
                "and l_receiptdate < date '1995-01-01' " +
                "group by " +
                "l_shipmode " +
                "order by " +
                "l_shipmode; ";
        Query[12] = "select " +
                " c_count, count(*) as custdist " +
                "from ( " +
                " select " +
                "  c_custkey, count(o_orderkey) c_count " +
                " from " +
                "  customer " +
                " left outer join orders  " +
                " on c_custkey = o_custkey " +
                " and o_comment not like '%special%requests%' " +
                " group by c_custkey " +
                ")as c_orders " +
                "group by c_count " +
                "order by custdist desc, c_count desc; ";
        Query[13] = "select " +
                "100.00 * sum(case " +
                "when p_type like 'PROMO%' " +
                "then l_extendedprice*(1-l_discount) " +
                "else 0 " +
                "end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue " +
                "from " +
                "lineitem, " +
                "part " +
                "where " +
                "l_partkey = p_partkey " +
                "and l_shipdate >= date '1995-09-01' " +
                "and l_shipdate < date '1995-10-01'; ";
        Query[14] = "DROP TABLE IF EXISTS revenue; " +
                "DROP TABLE IF EXISTS max_revenue; " +
                " " +
                "create table revenue(supplier_no int8, total_revenue double);  " +
                "create table max_revenue(max_revenue double); " +
                " " +
                "insert overwrite into revenue " +
                "select  " +
                " l_suppkey as supplier_no, " +
                " sum(l_extendedprice * (1 - l_discount)) as total_revenue " +
                "from  " +
                " lineitem " +
                "where  " +
                " l_shipdate >= '1996-01-01'::date " +
                " and l_shipdate < '1996-04-01'::date " +
                "group by " +
                " l_suppkey; " +
                " " +
                "insert overwrite into max_revenue " +
                "select  " +
                " max(total_revenue) " +
                "from  " +
                " revenue; " +
                " " +
                " " +
                "select  " +
                " s_suppkey, " +
                " s_name, " +
                " s_address, " +
                " s_phone, " +
                " total_revenue " +
                "from " +
                " supplier s join revenue r  " +
                " on  " +
                " s.s_suppkey = r.supplier_no " +
                " join max_revenue m  " +
                " on  " +
                " r.total_revenue = m.max_revenue " +
                "order " +
                " by s_suppkey; ";
        Query[15] = "select " +
                "p_brand, " +
                "p_type, " +
                "p_size, " +
                "count(distinct ps_suppkey) as supplier_cnt " +
                "from " +
                "partsupp, " +
                "part " +
                "where " +
                "p_partkey = ps_partkey " +
                "and p_brand <> 'Brand#45' " +
                "and p_type not like 'MEDIUM POLISHED%' " +
                "and p_size in (49, 14, 23, 45, 19, 3, 36, 9) " +
                "and ps_suppkey not in ( " +
                "select " +
                "s_suppkey " +
                "from " +
                "supplier " +
                "where " +
                "s_comment like '%Customer%Complaints%' " +
                ") " +
                "group by " +
                "p_brand, " +
                "p_type, " +
                "p_size " +
                "order by " +
                "supplier_cnt desc, " +
                "p_brand, " +
                "p_type, " +
                "p_size; ";
        Query[16] = "DROP TABLE IF EXISTS q17_small_quantity_order_revenue; " +
                "DROP TABLE IF EXISTS lineitem_tmp; " +
                " " +
                "create table q17_small_quantity_order_revenue (avg_yearly double); " +
                "create table lineitem_tmp (t_partkey int8, t_avg_quantity double); " +
                " " +
                "insert overwrite into lineitem_tmp " +
                "select  " +
                "  l_partkey as t_partkey, 0.2 * avg(l_quantity) as t_avg_quantity " +
                "from  " +
                "  lineitem " +
                "group by l_partkey; " +
                " " +
                "select " +
                "  sum(l_extendedprice) / 7.0 as avg_yearly " +
                "from " +
                "  (select l_quantity, l_extendedprice, t_avg_quantity from " +
                "   lineitem_tmp t join " +
                "     (select " +
                "        l_quantity, l_partkey, l_extendedprice " +
                "      from " +
                "        part p join lineitem l " +
                "        on " +
                "          p.p_partkey = l.l_partkey " +
                "          and p.p_brand = 'Brand#23' " +
                "          and p.p_container = 'MED BOX' " +
                "      ) l1 on l1.l_partkey = t.t_partkey " +
                "   ) a " +
                "where l_quantity < t_avg_quantity; ";
        Query[17] = "select " +
                "c_name, " +
                "c_custkey, " +
                "o_orderkey, " +
                "o_orderdate, " +
                "o_totalprice, " +
                "sum(l_quantity) " +
                "from " +
                "customer, " +
                "orders, " +
                "lineitem " +
                "where " +
                "o_orderkey in ( " +
                "select " +
                "l_orderkey " +
                "from " +
                "lineitem " +
                "group by " +
                "l_orderkey having " +
                "sum(l_quantity) > 300 " +
                ") " +
                "and c_custkey = o_custkey " +
                "and o_orderkey = l_orderkey " +
                "group by " +
                "c_name, " +
                "c_custkey, " +
                "o_orderkey, " +
                "o_orderdate, " +
                "o_totalprice " +
                "order by " +
                "o_totalprice desc, " +
                "o_orderdate " +
                "limit 100; ";
        Query[18] = "select " +
                "sum(l_extendedprice * (1 - l_discount) ) as revenue " +
                "from " +
                "lineitem, " +
                "part " +
                "where " +
                "( " +
                "p_partkey = l_partkey " +
                "and p_brand = 'Brand#12' " +
                "and p_container in ( 'SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') " +
                "and l_quantity >= 1 and l_quantity <= 1 + 10 " +
                "and p_size between 1 and 5 " +
                "and l_shipmode in ('AIR', 'AIR REG') " +
                "and l_shipinstruct = 'DELIVER IN PERSON' " +
                ") " +
                "or " +
                "( " +
                "p_partkey = l_partkey " +
                "and p_brand = 'Brand#23' " +
                "and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') " +
                "and l_quantity >= 10 and l_quantity <= 10 + 10 " +
                "and p_size between 1 and 10 " +
                "and l_shipmode in ('AIR', 'AIR REG') " +
                "and l_shipinstruct = 'DELIVER IN PERSON' " +
                ") " +
                "or " +
                "( " +
                "p_partkey = l_partkey " +
                "and p_brand = 'Brand#34' " +
                "and p_container in ( 'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') " +
                "and l_quantity >= 20 and l_quantity <= 20 + 10 " +
                "and p_size between 1 and 15 " +
                "and l_shipmode in ('AIR', 'AIR REG') " +
                "and l_shipinstruct = 'DELIVER IN PERSON' " +
                "); ";
        Query[19] = "DROP TABLE IF EXISTS q20_tmp1 ; " +
                "DROP TABLE IF EXISTS q20_tmp2 ; " +
                "DROP TABLE IF EXISTS q20_tmp3 ; " +
                "DROP TABLE IF EXISTS q20_tmp4 ; " +
                " " +
                "create table q20_tmp1(p_partkey int8); " +
                "create table q20_tmp2(l_partkey int8, l_suppkey int8, sum_quantity double); " +
                "create table q20_tmp3(ps_suppkey int8, ps_availqty int8, sum_quantity double); " +
                "create table q20_tmp4(ps_suppkey int8); " +
                " " +
                "insert overwrite into q20_tmp1 " +
                "select distinct p_partkey " +
                "from " +
                "  part  " +
                "where  " +
                "  p_name like 'forest%'; " +
                " " +
                "insert overwrite into q20_tmp2 " +
                "select  " +
                "  l_partkey, l_suppkey, 0.5 * sum(l_quantity) " +
                "from " +
                "  lineitem " +
                "where " +
                "  l_shipdate >= date '1994-01-01' " +
                "  and l_shipdate < date '1995-01-01' " +
                "group by l_partkey, l_suppkey; " +
                " " +
                "insert overwrite into q20_tmp3 " +
                "select  " +
                "  ps_suppkey, ps_availqty, sum_quantity " +
                "from   " +
                "  partsupp ps join q20_tmp1 t1  " +
                "  on  " +
                "    ps.ps_partkey = t1.p_partkey " +
                "  join q20_tmp2 t2  " +
                "  on  " +
                "    ps.ps_partkey = t2.l_partkey and ps.ps_suppkey = t2.l_suppkey; " +
                " " +
                "insert overwrite into q20_tmp4 " +
                "select  " +
                "  ps_suppkey " +
                "from  " +
                "  q20_tmp3 " +
                "where  " +
                "  ps_availqty > sum_quantity " +
                "group by ps_suppkey; " +
                " " +
                "select  " +
                "  s_name, s_address " +
                "from  " +
                "  supplier s join nation n " +
                "  on " +
                "    s.s_nationkey = n.n_nationkey " +
                "    and n.n_name = 'CANADA' " +
                "  join q20_tmp4 t4 " +
                "  on  " +
                "    s.s_suppkey = t4.ps_suppkey " +
                "order by s_name; ";
        Query[20] = "DROP TABLE IF EXISTS q21_tmp1; " +
                "DROP TABLE IF EXISTS q21_tmp2; " +
                "DROP TABLE IF EXISTS q21_suppliers_who_kept_orders_waiting; " +
                " " +
                "create table q21_tmp1(l_orderkey int8, count_suppkey int8, max_suppkey int); " +
                "create table q21_tmp2(l_orderkey int8, count_suppkey int8, max_suppkey int); " +
                "create table q21_suppliers_who_kept_orders_waiting(s_name text, numwait int); " +
                " " +
                "insert overwrite into q21_tmp1 " +
                "select " +
                "  l_orderkey, count(distinct l_suppkey), max(l_suppkey) as max_suppkey " +
                "from " +
                "  lineitem " +
                "group by l_orderkey; " +
                " " +
                "insert overwrite into q21_tmp2 " +
                "select " +
                "  l_orderkey, count(distinct l_suppkey), max(l_suppkey) as max_suppkey " +
                "from " +
                "  lineitem " +
                "where " +
                "  l_receiptdate > l_commitdate " +
                "group by l_orderkey; " +
                " " +
                "select " +
                "  s_name, count(1) as numwait " +
                "from " +
                "  (select s_name from " +
                "(select s_name, t2.l_orderkey, l_suppkey, count_suppkey, max_suppkey  " +
                " from q21_tmp2 t2 right outer join " +
                "      (select s_name, l_orderkey, l_suppkey from " +
                "         (select s_name, t1.l_orderkey, l_suppkey, count_suppkey, max_suppkey " +
                "          from " +
                "            q21_tmp1 t1 join " +
                "            (select s_name, l_orderkey, l_suppkey " +
                "             from  " +
                "               orders o join " +
                "               (select s_name, l_orderkey, l_suppkey " +
                "                from " +
                "                  nation n join supplier s " +
                "                  on " +
                "                    s.s_nationkey = n.n_nationkey " +
                "                    and n.n_name = 'SAUDI ARABIA' " +
                "                  join lineitem l " +
                "                  on " +
                "                    s.s_suppkey = l.l_suppkey " +
                "                where " +
                "                  l.l_receiptdate > l.l_commitdate " +
                "                ) l1 on o.o_orderkey = l1.l_orderkey and o.o_orderstatus = 'F' " +
                "             ) l2 on l2.l_orderkey = t1.l_orderkey " +
                "          ) a " +
                "          where " +
                "           (count_suppkey > 1) or ((count_suppkey=1) and (l_suppkey <> max_suppkey)) " +
                "       ) l3 on l3.l_orderkey = t2.l_orderkey " +
                "    ) b " +
                "    where " +
                "     (count_suppkey is null) or ((count_suppkey=1) and (l_suppkey = max_suppkey)) " +
                "  )c " +
                "group by s_name " +
                "order by numwait desc, s_name " +
                "limit 100; ";
        Query[21] = "select " +
                "cntrycode, count(*) as numcust, sum(c_acctbal) as totacctbal " +
                "from ( " +
                " select substr(c_phone,1,2) as cntrycode, c_acctbal, c_custkey " +
                " from customer " +
                " where " +
                " substr(c_phone,1,2) in ('13','31','23','29','30','18','17') " +
                " and c_acctbal > 5003.6857652151575 " +
                " " +
                ") as custsale " +
                "left join orders " +
                "on o_custkey = c_custkey " +
                "where o_custkey is null " +
                " " +
                "group by cntrycode " +
                "order by cntrycode; ";
    }

    public Pair<ResultSet, Long> Execute(TajoClient client, String q) throws TajoException {
        List<String> query = Arrays.asList(q.split(";"));
        Iterator<String> i = query.iterator();
        int count = 0;

        long startTime = System.currentTimeMillis();
        long duration;
        while(true) {
            count++;
            if(count == query.size()-1) {
                Object element = i.next();
                client.executeQuery(element.toString() + ";");
                duration = System.currentTimeMillis() - startTime;
                return new Pair<ResultSet, Long>(client.executeQuery(element.toString() + ";"), duration);
            }
            else {
                client.executeQuery(i.next() + ";");
            }
        }
    }

    public String Q1(TajoClient client) throws TajoException, SQLException {

        Pair<ResultSet, Long> Result = Execute(client, Query[0]);
        ResultSet res = Result.getFirst();
        Long duration = Result.getSecond();

        while(res.next()) {
            if (res.getString(1).equals("A")
                    && res.getString(2).equals("F")
                    && res.getDouble(3) == 37734107.00
                    && ((56586554400.72 < res.getDouble(4) && (res.getDouble(4) < 56586554400.74)))
                    && ((53758257134.86 < res.getDouble(5)) && (res.getDouble(5) < 53758257134.88))
                    && ((55909065222.82 < res.getDouble(6)) && (res.getDouble(6) < 55909065222.84))
                    && ((25.51 < res.getDouble(7)) && (res.getDouble(7) < 25.53))
                    && ((38273.12 < res.getDouble(8)) && (res.getDouble(8) < 38273.14))
                    && ((0.04 < res.getDouble(9)) && (res.getDouble(9) < 0.06))
                    && res.getInt(10) == 1478493) {
                NumberFormat formatter = new DecimalFormat("#0.00000");
                return "CORRECT! Execution time : " + formatter.format(duration / 1000d) + " seconds";
            }
        }

        return "INCORRECT!";
    }

    public String Q2(TajoClient client) throws TajoException, SQLException {

        Pair<ResultSet, Long> Result = Execute(client, Query[1]);
        ResultSet res = Result.getFirst();
        Long duration = Result.getSecond();

        while(res.next()) {
            if (res.getDouble(1) == 9938.53
                    && res.getString(2).equals("Supplier#000005359")
                    && res.getString(3).equals("UNITED KINGDOM")
                    && res.getInt(4) == 185358
                    && res.getString(5).equals("Manufacturer#4")
                    && res.getString(6).equals("QKuHYh,vZGiwu2FWEJoLDx04")
                    && res.getString(7).equals("33-429-790-6131")
                    && res.getString(8).equals("uriously regular requests hag")) {
                NumberFormat formatter = new DecimalFormat("#0.00000");
                return "CORRECT! Execution time : " + formatter.format(duration / 1000d) + " seconds";
            }
        }

        return "INCORRECT!";
    }

    public String Q3(TajoClient client) throws TajoException, SQLException {

        Pair<ResultSet, Long> Result = Execute(client, Query[2]);
        ResultSet res = Result.getFirst();
        Long duration = Result.getSecond();

        while(res.next()) {
            if (res.getInt(1) == 2456423
                    && (406181.00 < res.getDouble(2) && res.getDouble(2) < 406181.02)
                    && res.getDate(3).toString().equals("1995-03-05")
                    && res.getInt(4) == 0) {
                NumberFormat formatter = new DecimalFormat("#0.00000");
                return "CORRECT! Execution time : " + formatter.format(duration / 1000d) + " seconds";
            }
        }

        return "INCORRECT!";
    }

    public String Q4(TajoClient client) throws TajoException, SQLException {

        Pair<ResultSet, Long> Result = Execute(client, Query[3]);
        ResultSet res = Result.getFirst();
        Long duration = Result.getSecond();

        while(res.next()) {
            if (res.getString(1).equals("1-URGENT")
                    && res.getInt(2) == 10594) {
                NumberFormat formatter = new DecimalFormat("#0.00000");
                return "CORRECT! Execution time : " + formatter.format(duration / 1000d) + " seconds";
            }
        }

        return "INCORRECT!";
    }

    public String Q5(TajoClient client) throws TajoException, SQLException {

        Pair<ResultSet, Long> Result = Execute(client, Query[4]);
        ResultSet res = Result.getFirst();
        Long duration = Result.getSecond();

        while(res.next()) {
            if (res.getString(1).equals("INDONESIA")
                    && (55502041.16 < res.getDouble(2) && res.getDouble(2) < 55502041.18)) {
                NumberFormat formatter = new DecimalFormat("#0.00000");
                return "CORRECT! Execution time : " + formatter.format(duration / 1000d) + " seconds";
            }
        }

        return "INCORRECT!";
    }

    public String Q6(TajoClient client) throws TajoException, SQLException {

        Pair<ResultSet, Long> Result = Execute(client, Query[5]);
        ResultSet res = Result.getFirst();
        Long duration = Result.getSecond();

        while(res.next()) {
            if (123141078.22 < res.getDouble(1) && res.getDouble(1) < 123141078.24) {
                NumberFormat formatter = new DecimalFormat("#0.00000");
                return "CORRECT! Execution time : " + formatter.format(duration / 1000d) + " seconds";
            }
        }

        return "INCORRECT!";
    }

    public String Q7(TajoClient client) throws TajoException, SQLException {

        Pair<ResultSet, Long> Result = Execute(client, Query[6]);
        ResultSet res = Result.getFirst();
        Long duration = Result.getSecond();

        while(res.next()) {
            if (res.getString(1).equals("FRANCE")
                    && res.getString(2).equals("GERMANY")
                    && res.getInt(3) == 1995
                    && (54639732.72 < res.getDouble(4) && res.getDouble(4) < 54639732.74)) {
                NumberFormat formatter = new DecimalFormat("#0.00000");
                return "CORRECT! Execution time : " + formatter.format(duration / 1000d) + " seconds";
            }
        }

        return "INCORRECT!";
    }

    public String Q8(TajoClient client) throws TajoException, SQLException {

        Pair<ResultSet, Long> Result = Execute(client, Query[7]);
        ResultSet res = Result.getFirst();
        Long duration = Result.getSecond();

        while(res.next()) {
            if (res.getInt(1) == 1995
                    && (0.02 < res.getDouble(2) && res.getDouble(2) < 0.04)) {
                NumberFormat formatter = new DecimalFormat("#0.00000");
                return "CORRECT! Execution time : " + formatter.format(duration / 1000d) + " seconds";
            }
        }

        return "INCORRECT!";
    }

    public String Q9(TajoClient client) throws TajoException, SQLException {

        Pair<ResultSet, Long> Result = Execute(client, Query[8]);
        ResultSet res = Result.getFirst();
        Long duration = Result.getSecond();

        while(res.next()) {
            if (res.getString(1).equals("ALGERIA")
                    && res.getInt(2) == 1998
                    && (31342867.23 < res.getDouble(3) && res.getDouble(3) < 31342867.24)) {
                NumberFormat formatter = new DecimalFormat("#0.00000");
                return "CORRECT! Execution time : " + formatter.format(duration / 1000d) + " seconds";
            }
        }

        return "INCORRECT!";
    }

    public String Q10(TajoClient client) throws TajoException, SQLException {

        Pair<ResultSet, Long> Result = Execute(client, Query[9]);
        ResultSet res = Result.getFirst();
        Long duration = Result.getSecond();

        while(res.next()) {
            if (res.getInt(1) == 57040
                    && res.getString(2).equals("Customer#000057040")
                    && (734235.23 < res.getDouble(3) && res.getDouble(3) < 734235.25)
                    && (632.86 < res.getDouble(4) && res.getDouble(4) < 632.88)
                    && res.getString(5).equals("JAPAN")
                    && res.getString(6).equals("Eioyzjf4pp")
                    && res.getString(7).equals("22-895-641-3466")
                    && res.getString(8).equals("sits. slyly regular requests sleep alongside of the regular inst")) {
                NumberFormat formatter = new DecimalFormat("#0.00000");
                return "CORRECT! Execution time : " + formatter.format(duration / 1000d) + " seconds";
            }
        }

        return "INCORRECT!";
    }

    public String Q11(TajoClient client) throws TajoException, SQLException {

        Pair<ResultSet, Long> Result = Execute(client, Query[10]);
        ResultSet res = Result.getFirst();
        Long duration = Result.getSecond();

        while(res.next()) {
            if (res.getInt(1) == 129760
                    && (17538456.85 < res.getDouble(2) && res.getDouble(2) < 17538456.87)) {
                NumberFormat formatter = new DecimalFormat("#0.00000");
                return "CORRECT! Execution time : " + formatter.format(duration / 1000d) + " seconds";
            }
        }

        return "INCORRECT!";
    }

    public String Q12(TajoClient client) throws TajoException, SQLException {

        Pair<ResultSet, Long> Result = Execute(client, Query[11]);
        ResultSet res = Result.getFirst();
        Long duration = Result.getSecond();

        while(res.next()) {
            if (res.getString(1).equals("MAIL")
                    && res.getInt(2) == 6202
                    && res.getInt(3) == 9324) {
                NumberFormat formatter = new DecimalFormat("#0.00000");
                return "CORRECT! Execution time : " + formatter.format(duration / 1000d) + " seconds";
            }
        }

        return "INCORRECT!";
    }

    public String Q13(TajoClient client) throws TajoException, SQLException {

        Pair<ResultSet, Long> Result = Execute(client, Query[12]);
        ResultSet res = Result.getFirst();
        Long duration = Result.getSecond();

        while(res.next()) {
            if (res.getInt(1) == 9
                    && res.getInt(2) == 6641) {
                NumberFormat formatter = new DecimalFormat("#0.00000");
                return "CORRECT! Execution time : " + formatter.format(duration / 1000d) + " seconds";
            }
        }

        return "INCORRECT!";
    }

    public String Q14(TajoClient client) throws TajoException, SQLException {

        Pair<ResultSet, Long> Result = Execute(client, Query[13]);
        ResultSet res = Result.getFirst();
        Long duration = Result.getSecond();

        while(res.next()) {
            if (16.37 < res.getDouble(1) && res.getDouble(1) < 16.39) {
                NumberFormat formatter = new DecimalFormat("#0.00000");
                return "CORRECT! Execution time : " + formatter.format(duration / 1000d) + " seconds";
            }
        }

        return "INCORRECT!";
    }

    public String Q15(TajoClient client) throws TajoException, SQLException {

        Pair<ResultSet, Long> Result = Execute(client, Query[14]);
        ResultSet res = Result.getFirst();
        Long duration = Result.getSecond();

        while(res.next()) {
            if (res.getInt(1) == 8449
                    && res.getString(2).equals("Supplier#000008449")
                    && res.getString(3).equals("Wp34zim9qYFbVctdW")
                    && res.getString(4).equals("20-469-856-8873")
                    && (1772627.20 < res.getDouble(5) && res.getDouble(5) < 1772627.22)) {
                NumberFormat formatter = new DecimalFormat("#0.00000");
                return "CORRECT! Execution time : " + formatter.format(duration / 1000d) + " seconds";
            }
        }

        return "INCORRECT!";
    }

    public String Q16(TajoClient client) throws TajoException, SQLException {

        Pair<ResultSet, Long> Result = Execute(client, Query[15]);
        ResultSet res = Result.getFirst();
        Long duration = Result.getSecond();

        while(res.next()) {
            if (res.getString(1).equals("Brand#41")
                    && res.getString(2).equals("MEDIUM BRUSHED TIN")
                    && res.getInt(3) == 3
                    && res.getInt(4) == 28) {
                NumberFormat formatter = new DecimalFormat("#0.00000");
                return "CORRECT! Execution time : " + formatter.format(duration / 1000d) + " seconds";
            }
        }

        return "INCORRECT!";
    }

    public String Q17(TajoClient client) throws TajoException, SQLException {

        Pair<ResultSet, Long> Result = Execute(client, Query[16]);
        ResultSet res = Result.getFirst();
        Long duration = Result.getSecond();

        while(res.next()) {
            if (348406.04 < res.getDouble(1) && res.getDouble(1) < 348406.06) {
                NumberFormat formatter = new DecimalFormat("#0.00000");
                return "CORRECT! Execution time : " + formatter.format(duration / 1000d) + " seconds";
            }
        }

        return "INCORRECT!";
    }

    public String Q18(TajoClient client) throws TajoException, SQLException {

        Pair<ResultSet, Long> Result = Execute(client, Query[17]);
        ResultSet res = Result.getFirst();
        Long duration = Result.getSecond();

        while(res.next()) {
            if (res.getString(1).equals("Customer#000128120")
                    && res.getInt(2) == 128120
                    && res.getInt(3) == 4722021
                    && res.getDate(4).toString().equals("1994-04-07")
                    && res.getDouble(5) == 544089.09
                    && res.getDouble(6) == 323.00) {
                NumberFormat formatter = new DecimalFormat("#0.00000");
                return "CORRECT! Execution time : " + formatter.format(duration / 1000d) + " seconds";
            }
        }

        return "INCORRECT!";
    }

    public String Q19(TajoClient client) throws TajoException, SQLException {

        Pair<ResultSet, Long> Result = Execute(client, Query[18]);
        ResultSet res = Result.getFirst();
        Long duration = Result.getSecond();

        while(res.next()) {
            if (3083843.04 < res.getDouble(1) && res.getDouble(1) < 3083843.06) {
                NumberFormat formatter = new DecimalFormat("#0.00000");
                return "CORRECT! Execution time : " + formatter.format(duration / 1000d) + " seconds";
            }
        }

        return "INCORRECT!";
    }

    public String Q20(TajoClient client) throws TajoException, SQLException {

        Pair<ResultSet, Long> Result = Execute(client, Query[19]);
        ResultSet res = Result.getFirst();
        Long duration = Result.getSecond();

        while(res.next()) {
            if (res.getString(1).equals("Supplier#000000020")
                    && res.getString(2).equals("iybAE,RmTymrZVYaFZva2SH,j")) {
                NumberFormat formatter = new DecimalFormat("#0.00000");
                return "CORRECT! Execution time : " + formatter.format(duration / 1000d) + " seconds";
            }
        }

        return "INCORRECT!";
    }

    public String Q21(TajoClient client) throws TajoException, SQLException {

        Pair<ResultSet, Long> Result = Execute(client, Query[20]);
        ResultSet res = Result.getFirst();
        Long duration = Result.getSecond();

        while(res.next()) {
            if (res.getString(1).equals("Supplier#000002829")
                    && res.getInt(2) == 20) {
                NumberFormat formatter = new DecimalFormat("#0.00000");
                return "CORRECT! Execution time : " + formatter.format(duration / 1000d) + " seconds";
            }
        }

        return "INCORRECT!";
    }

    public String Q22(TajoClient client) throws TajoException, SQLException {

        Pair<ResultSet, Long> Result = Execute(client, Query[21]);
        ResultSet res = Result.getFirst();
        Long duration = Result.getSecond();

        while(res.next()) {
            if (res.getInt(1) == 13
                    && res.getInt(2) == 888
                    && (6737713.98 < res.getDouble(3) && res.getDouble(3) < 6737714.00)) {
                NumberFormat formatter = new DecimalFormat("#0.00000");
                return "CORRECT! Execution time : " + formatter.format(duration / 1000d) + " seconds";
            }
        }

        return "INCORRECT!";
    }

    public void run(TajoClient client) {

        try {
            System.out.println("Q01 ----- " + Q1(client));
            System.out.println("Q02 ----- " + Q2(client));
            System.out.println("Q03 ----- " + Q3(client));
            System.out.println("Q04 ----- " + Q4(client));
            System.out.println("Q05 ----- " + Q5(client));
            System.out.println("Q06 ----- " + Q6(client));
            System.out.println("Q07 ----- " + Q7(client));
            System.out.println("Q08 ----- " + Q8(client));
            System.out.println("Q09 ----- " + Q9(client));
            System.out.println("Q10 ----- " + Q10(client));
            System.out.println("Q11 ----- " + Q11(client));
            System.out.println("Q12 ----- " + Q12(client));
            System.out.println("Q13 ----- " + Q13(client));
            System.out.println("Q14 ----- " + Q14(client));
            System.out.println("Q15 ----- " + Q15(client));
            System.out.println("Q16 ----- " + Q16(client));
            System.out.println("Q17 ----- " + Q17(client));
            System.out.println("Q18 ----- " + Q18(client));
            System.out.println("Q19 ----- " + Q19(client));
            System.out.println("Q20 ----- " + Q20(client));
            System.out.println("Q21 ----- " + Q21(client));
            System.out.println("Q22 ----- " + Q22(client));
        } catch (QueryFailedException e) {
            System.err.print("FAILED.");
        } catch (QueryKilledException e) {
            System.err.print("KILLED.");
        } catch (Exception e) {
            System.err.println("OTHER EXCEPTION.");
            e.printStackTrace();
        }
    }

    public int Driver() throws ClientUnableToConnectException, IOException, UndefinedDatabaseException {
        Scanner s = new Scanner(System.in);

        System.out.print("hostname : ");
        String hostname = s.nextLine();
        System.out.print("port : ");
        int port = Integer.valueOf(s.nextLine());
        TajoClient client = new TajoClient(hostname, port);

        System.out.print("database : ");
        client.selectDB(s.nextLine());

        System.out.println("\nTPC-H START\n");

        run(client);

        System.out.println("\nALL TEST IS DONE");
        client.close();

        return 0;
    }

    public static void main(String args[]) throws ClientUnableToConnectException, IOException, UndefinedDatabaseException {
        TajoTPCH t = new TajoTPCH();
        System.exit(t.Driver());
    }
}
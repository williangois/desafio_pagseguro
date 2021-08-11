select 
	fmk.mes, fmk_g.mes as mes_g,
	fmk.merchant, fmk_g.merchant as merchant_g,
	fmk.tpv, fmk_g.tpv as tpv_g,
	fmk.qtd_transacoes, fmk_g.qtd_transacoes as qtd_transacoes_g 
from analytic.fact_merchant_kpi fmk 
left join (
	select t.mes, t.merchant, sum(t.amount) as tpv, count(0) as qtd_transacoes from db.transactions t
	where t.merchant = 'M348934600'
	group by t.mes, t.merchant
) fmk_g on fmk_g.mes=fmk.mes and fmk_g.merchant=fmk.merchant 
where fmk.merchant = 'M348934600';

select * from (
	select dmc.category, ROUND(sum(tpv)/sum(qtd_transacoes), 2) ticket_medio from analytic.fact_merchant_kpi fmk
	left join analytic.dim_merchant_category dmc on dmc.id_merchant = fmk.id_merchant 
	group by dmc.category
) x
order by x.ticket_medio desc;
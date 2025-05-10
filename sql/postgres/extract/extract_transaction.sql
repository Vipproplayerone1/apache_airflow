select * from transactions
left join order on transactions.order_id = order.id
left join topup on transactions.topup_id = topup.id
where 1=1
{condition};
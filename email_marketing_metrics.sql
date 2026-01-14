-- Основна таблиця з аккаунтами і email-метриками
with base_data as (
   -- accounts метрики
   select
      s.date,
      sp.country,
      a.send_interval,
      a.is_verified,
      a.is_unsubscribed,
      count(distinct a.id) as account_cnt,
      0 as sent_msg,
      0 as open_msg,
      0 as visit_msg
   from `DA.account` a
   join `DA.account_session` acs
      on a.id = acs.account_id
   join `DA.session` s
      on acs.ga_session_id = s.ga_session_id
   join `DA.session_params` sp
      on s.ga_session_id = sp.ga_session_id
   group by 1,2,3,4,5


   union all


   -- email метрики
   select
      date_add(s.date, interval sent_date DAY) as date,
      sp.country,
      a.send_interval,
      a.is_verified,
      a.is_unsubscribed,
      0 as account_cnt,
      count(distinct es.id_message) as sent_msg,
      count(distinct eo.id_message) as open_msg,
      count(distinct ev.id_message) as visit_msg
   from `DA.email_sent` es
   left join `DA.email_open` eo
      on es.id_message = eo.id_message
   left join `DA.email_visit` ev
      on es.id_message = ev.id_message
   join `DA.account` a
      on es.id_account = a.id
   join `DA.account_session` acs
      on a.id = acs.account_id
   join `DA.session` s
      on acs.ga_session_id = s.ga_session_id
   join `DA.session_params` sp
      on s.ga_session_id = sp.ga_session_id
   group by 1,2,3,4,5
),


-- Агрегація після union all
aggregated_base_data as (
    select
        date,
        country,
        send_interval,
        is_verified,
        is_unsubscribed,
        sum(account_cnt) as account_cnt,
        sum(sent_msg) as sent_msg,
        sum(open_msg) as open_msg,
        sum(visit_msg) as visit_msg
    from base_data
    group by
        date,
        country,
        send_interval,
        is_verified,
        is_unsubscribed
),


-- total по країнах
country_accounts as (
   select
      sp.country,
      count(distinct a.id) as total_country_account_cnt
   from `DA.account` a
   join `DA.account_session` acs on a.id = acs.account_id
   join `DA.session` s on acs.ga_session_id = s.ga_session_id
   join `DA.session_params` sp on s.ga_session_id = sp.ga_session_id
   group by sp.country
),
country_emails as (
   select
      sp.country,
      count(distinct es.id_message) as total_country_sent_cnt
   from `DA.email_sent` es
   join `DA.account` a on es.id_account = a.id
   join `DA.account_session` acs on a.id = acs.account_id
   join `DA.session` s on acs.ga_session_id = s.ga_session_id
   join `DA.session_params` sp on s.ga_session_id = sp.ga_session_id
   group by sp.country
),


-- Об'єднання total по країнам
combined_totals as (
   select
      coalesce(ca.country, ce.country) as country,
      coalesce(ca.total_country_account_cnt, 0) as total_country_account_cnt,
      coalesce(ce.total_country_sent_cnt, 0) as total_country_sent_cnt
   from country_accounts ca
   full outer join country_emails ce
      on ca.country = ce.country
),


-- Додання рангів
country_ranked as (
   select *,
      dense_rank() over(order by total_country_account_cnt desc) as rank_total_country_account_cnt,
      dense_rank() over(order by total_country_sent_cnt desc) as rank_total_country_sent_cnt
   from combined_totals
)


-- Фінальний результат
select
   abd.*,
   cr.total_country_account_cnt,
   cr.total_country_sent_cnt,
   cr.rank_total_country_account_cnt,
   cr.rank_total_country_sent_cnt
from aggregated_base_data abd
left join country_ranked cr
   on abd.country = cr.country
where cr.rank_total_country_sent_cnt <= 10
   or cr.rank_total_country_account_cnt <= 10
order by abd.date;

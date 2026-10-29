"""Genera el tablero provisionado. Ejecutar dentro del contenedor Python; JSON a stdout."""
import json

PG={'type':'postgres','uid':'cryptopulse-postgres'}
PROM={'type':'prometheus','uid':'cryptopulse-prometheus'}
panels=[]
def panel(id,title,type,x,y,w,h,query=None,unit='short',description='',prom=False):
    p={'id':id,'title':title,'type':type,'gridPos':dict(x=x,y=y,w=w,h=h),'description':description,
       'fieldConfig':{'defaults':{'unit':unit,'noValue':'Pendiente'},'overrides':[]},'options':{}}
    if query:
        p['datasource']=PROM if prom else PG
        p['targets']=[dict(refId='A',expr=query,instant=type=='stat',legendFormat='{{query}}{{symbol}}{{topic}}') if prom else
         dict(refId='A',datasource=PG,rawSql=query,format='time_series' if type in ('stat','timeseries') else 'table',rawQuery=True)]
    if type=='stat':
        p['options']={'reduceOptions':{'calcs':['lastNotNull'],'fields':'','values':False},'textMode':'value_and_name','colorMode':'background','graphMode':'none'}
        # Prices and counts must not inherit Grafana's generic red threshold at 80.
        p['fieldConfig']['defaults']['thresholds']={'mode':'absolute','steps':[{'color':'blue','value':None}]}
    elif type=='timeseries':
        p['fieldConfig']['defaults'].update(custom={'drawStyle':'line','lineWidth':2,'fillOpacity':6,'spanNulls':False,'showPoints':'auto'})
        p['options']={'legend':{'displayMode':'list','placement':'bottom'},'tooltip':{'mode':'multi','sort':'desc'}}
    elif type=='table':
        p['options']={'showHeader':True,'cellHeight':'md','footer':{'show':False}}
        p['fieldConfig']['defaults']['custom']={'align':'left','cellOptions':{'type':'auto'},'filterable':True,'wrapText':True}
    panels.append(p)
    return p
def row(id,title,y): panel(id,title,'row',0,y,24,1)
def table(id,title,y,h,query,description='',x=0,w=24):
    p=panel(id,title,'table',x,y,w,h,query,description=description)
    p['fieldConfig']['overrides'].append({'matcher':{'id':'byName','options':'Estado'},'properties':[
      {'id':'custom.cellOptions','value':{'type':'color-background'}},
      {'id':'mappings','value':[{'type':'value','options':{s:{'text':s,'color':c} for s,c in
       [('NORMAL','green'),('LISTO','green'),('VIGILANCIA','orange'),('ALERTA','red'),('DESACTUALIZADO','red'),('SIN EVALUAR','gray'),('REFERENCIA','blue'),('DATOS INCOMPLETOS','red')]}}]}]})
    return p
TF='(as_of_ms BETWEEN $__unixEpochFrom()::bigint*1000 AND $__unixEpochTo()::bigint*1000)'
VER="model_version=(SELECT value FROM runtime_configuration WHERE key='active_model_version')"
LIVE="source_mode='live' AND "+VER

p=panel(30,'Estado integral del producto','stat',0,0,6,5,
 'cryptopulse_product_ready',prom=True,
 description='LISTO exige productor y driver recientes, continuidad de 61 velas en ambos activos, siete pronósticos de la versión activa con menos de 3 minutos y retraso acotado. Un contenedor activo no basta.')
p['fieldConfig']['defaults']['mappings']=[{'type':'value','options':{'1':{'text':'LISTO','color':'green'},'0':{'text':'REVISAR DATOS / INFERENCIA','color':'red'}}}]
p['fieldConfig']['defaults']['thresholds']={'mode':'absolute','steps':[{'color':'red','value':None},{'color':'green','value':1}]}
for id,symbol,x in [(2,'BTCUSDT',6),(3,'SOLUSDT',12)]:
    panel(id,f'{symbol} · cierre de 1 minuto','stat',x,0,6,5,f"SELECT now() AS time,close::double precision AS value FROM market_candles WHERE symbol='{symbol}' AND source_mode<>'replay' ORDER BY open_ms DESC LIMIT 1",'suffix: USDT','Último cierre confirmado; verificar frescura antes de usarlo. USDT es el activo de cotización.')
panel(31,'Pronósticos vigentes · objetivo 7/7','stat',18,0,6,5,"SELECT now() AS time,count(*)::double precision AS value FROM cp_signal_board WHERE kind='forecast' AND age_seconds<=180",description='BTC y SOL: volatilidad, caída y anomalía; un pronóstico conjunto SOL−BTC. Frescura máxima: 180 segundos.')
table(32,'Frescura y continuidad por activo',5,5,"SELECT symbol AS \"Activo\",status AS \"Estado\",round(candle_age_seconds,1) AS \"Edad vela · s\",context_rows AS \"Velas contexto / 61\",missing_minutes AS \"Minutos faltantes\",round(kpi_age_seconds,1) AS \"Edad indicadores · s\",to_timestamp(last_close/1000.0) AS \"Último cierre\" FROM cp_asset_health ORDER BY symbol")
p=panel(33,'Cómo utilizar CryptoPulse','text',0,10,24,4)
p['options']={'mode':'markdown','content':'**Monitoreo de riesgos de BTC y SOL · Binance Spot**  \n1. Comprueba **LISTO** y **7/7**. 2. Revisa el estado observado y el pronóstico por dimensión. 3. Contrasta señales y registra su evolución.  \n**NORMAL** = evaluación reciente sin cruce; **VIGILANCIA / ALERTA** = cruce real del umbral; **SIN EVALUAR / DESACTUALIZADO** = no interpretar como ausencia de riesgo. Las señales apoyan la revisión; no ejecutan órdenes. Horizontes: 5, 15 y 30 minutos. Tiempos mostrados según la zona del tablero.'}
row(40,'Centro de señales · diagnóstico y revisión',14)
table(10,'Señales de mercado · estado, umbral y explicación',15,11,
 "SELECT symbol AS \"Activo\",CASE dimension WHEN 'volatility' THEN 'Volatilidad' WHEN 'crash' THEN 'Caída' WHEN 'anomaly' THEN 'Actividad' ELSE 'Divergencia' END AS \"Dimensión\",CASE kind WHEN 'observed' THEN 'Observada' ELSE 'Pronóstico' END AS \"Origen\",state AS \"Estado\",round(value::numeric,5) AS \"Valor\",round(threshold::numeric,5) AS \"Umbral\",unit AS \"Unidad\",round(age_seconds::numeric,0) AS \"Edad · s\",explanation AS \"Interpretación\",action AS \"Qué revisar\" FROM cp_signal_board ORDER BY dimension,symbol,kind",
 'Siempre muestra las 14 evaluaciones posibles. NORMAL no es un dato ausente. Probabilidades en escala 0–1; las unidades figuran en cada fila. Señales observadas y predictivas se distinguen explícitamente.')
history=f'''WITH events AS (SELECT to_timestamp(as_of_ms/1000.0) AS "Fecha",symbol AS "Activo",dimension AS "Dimensión",kind AS "Origen",CASE severity WHEN 'high' THEN 'ALERTA' ELSE 'VIGILANCIA' END AS "Estado",value AS "Valor",threshold AS "Umbral",explanation AS "Detalle",action AS "Qué revisar",model_version AS "Versión" FROM market_signals WHERE triggered AND {LIVE} AND {TF} ORDER BY as_of_ms DESC LIMIT 100)
 SELECT * FROM events UNION ALL SELECT NULL,'BTC / SOL','Todas','Estado',CASE WHEN (SELECT count(*) FROM cp_signal_board WHERE kind='forecast' AND age_seconds<=180)=7 THEN 'SIN CRUCES' ELSE 'REVISAR INFERENCIA' END,NULL,NULL,'No hay señales que superen umbrales en el período seleccionado. Comprueba el estado actual arriba.','No se generan alertas artificiales para llenar esta tabla.',NULL WHERE NOT EXISTS(SELECT 1 FROM events)'''
table(34,'Historial de cruces · período seleccionado',26,8,history,'Solo cruces reales. Muestra una explicación cuando no hay alertas; no confundir un vacío histórico con salud del pipeline.')
row(41,'Cuatro dimensiones · observado, pronóstico y realidad posterior',34)
for id,dim,title,x,y,unit in [(5,'volatility','Volatilidad · observada y futura a 15 min',0,35,'percent'),(6,'crash','Caída significativa · probabilidad a 30 min',12,35,'percentunit'),(7,'anomaly','Actividad anómala · probabilidad a 5 min',0,44,'percentunit'),(8,'divergence','SOL menos BTC · divergencia a 15 min',12,44,'suffix: p.p.')]:
    series=[('pronóstico','forecast')]
    if dim in ('volatility','divergence'): series=[('observada reciente','observed'),('pronóstico','forecast'),('realidad futura','actual')]
    sql=' UNION ALL '.join(f"SELECT to_timestamp(as_of_ms/1000.0) AS time,symbol||' · {label}' AS metric,{col} AS value FROM predictions WHERE dimension='{dim}' AND {LIVE} AND {TF}"+(" AND actual IS NOT NULL" if col=='actual' else '') for label,col in series)+' ORDER BY 1'
    descriptions={'volatility':'Volatilidad realizada = 100√Σ(log-retorno²), sin anualizar. La realidad futura se alinea al origen del pronóstico; solo existe después de 15 minutos completos.',
     'crash':'Probabilidad de rendimiento al cierre de 30 minutos bajo P5 histórico: BTC −0,558 %; SOL −1,116 %. No estima el mínimo intraintervalo. Modelo limitado: consultar precisión y recall históricos.',
     'anomaly':'Probabilidad de superar P95 de volumen u operaciones en los siguientes 5 minutos, por activo y hora UTC. La anomalía observada se muestra separada en el centro de señales.',
     'divergence':'Puntos porcentuales de rendimiento SOL menos BTC. El regresor candidato no superó la referencia: pronóstico desplegado cero. La señal OBSERVADA detecta desacoplamientos reales.'}
    p=panel(id,title,'timeseries',x,y,12,9,sql,unit,descriptions[dim])
    if dim in ('crash','anomaly'): p['fieldConfig']['defaults'].update(min=0,max=1)
panel(9,'Correlación BTC / SOL · rendimientos 60 min','timeseries',0,53,12,8,f"SELECT to_timestamp(as_of_ms/1000.0) AS time,(metrics->>'correlation_60m')::double precision AS value FROM market_kpis WHERE symbol='BTC-SOL' AND {TF} ORDER BY 1",description='Pearson sobre rendimientos consecutivos de un minuto: −1 a +1. Correlación no implica causalidad.')['fieldConfig']['defaults'].update(min=-1,max=1)
table(99,'Actividad observada · referencia por hora UTC',53,8,"SELECT DISTINCT ON(symbol) symbol AS \"Activo\",to_timestamp(as_of_ms/1000.0) AS \"Cierre\",round((metrics->>'quote5')::numeric,0) AS \"USDT · 5 min\",round((metrics->>'quote5_p95')::numeric,0) AS \"USDT · P95\",(metrics->>'trades5')::bigint AS \"Operaciones · 5 min\",round((metrics->>'trades5_p95')::numeric,0) AS \"Operaciones · P95\",round((metrics->>'volume_zscore')::numeric,2) AS \"Z volumen\" FROM market_kpis WHERE symbol IN ('BTCUSDT','SOLUSDT') ORDER BY symbol,as_of_ms DESC",'P95 y Z score respecto del entrenamiento 2022–2024 para activo/hora UTC. Comprueba frescura arriba; no atribuye fraude.',x=12,w=12)
row(42,'Evidencia de utilidad · errores y referencias',61)
table(11,'Prueba histórica reservada · modelo contra referencia',62,8,
 "SELECT m.dimension AS \"Dimensión\",m.method AS \"Método desplegado\",(m.metrics->>'n')::bigint AS \"Casos\",(m.metrics->>'mae')::float AS \"MAE\",(m.metrics->>'rmse')::float AS \"RMSE\",(b.metrics->>'rmse')::float AS \"RMSE referencia\",(m.metrics->>'pr_auc')::float AS \"PR AUC\",(m.metrics->>'brier')::float AS \"Brier\",(b.metrics->>'brier')::float AS \"Brier referencia\",(m.metrics->>'precision')::float AS \"Precisión\",(m.metrics->>'recall')::float AS \"Recall\",m.model_version AS \"Versión\" FROM model_metrics m JOIN model_metrics b ON b.dimension=m.dimension AND b.model_version=m.model_version AND b.split='test_baseline' WHERE m.split='test_deployed' AND m.model_version=(SELECT value FROM runtime_configuration WHERE key='active_model_version') ORDER BY m.dimension",
 'Prueba enero–septiembre 2026, muestreo cada 15 min. MAE/RMSE en unidad de su objetivo; Brier menor es mejor. Precisión y recall son proporciones. No se reentrena con esta prueba para escoger modelos.')
table(12,'Evaluación en vivo · activo, dimensión y período seleccionado',70,8,
 f"SELECT dimension AS \"Dimensión\",symbol AS \"Activo\",count(*) AS \"Emitidas\",count(actual) AS \"Evaluadas\",count(*) FILTER(WHERE actual IS NULL) AS \"Pendientes\",avg(abs(forecast-actual)) FILTER(WHERE dimension IN ('volatility','divergence')) AS \"MAE regresión\",sqrt(avg(power(forecast-actual,2)) FILTER(WHERE dimension IN ('volatility','divergence'))) AS \"RMSE regresión\",avg(power(forecast-actual,2)) FILTER(WHERE dimension IN ('crash','anomaly')) AS \"Brier clasificación\",avg(power(baseline-actual,2)) AS \"MSE referencia\" FROM predictions WHERE {LIVE} AND {TF} GROUP BY dimension,symbol ORDER BY dimension,symbol",
 'Solo la versión activa y el período elegido. Los horizontes todavía abiertos o con datos incompletos permanecen pendientes. Clasificación usa Brier; no se mezcla con RMSE de precios.')
row(43,'Operación · recepción, procesamiento y persistencia',78)
for id,title,x,expr,unit,desc in [(13,'WebSocket · 1 conectado',0,'cryptopulse_producer_connected','short','La conexión no sustituye continuidad e inferencia.'),(14,'Spark · edad de señal de vida',6,'cryptopulse_stream_heartbeat_age_seconds','s','La alerta integral comprueba además pronósticos.'),(15,'Kafka · eventos pendientes',12,'sum(cryptopulse_kafka_checkpoint_lag)','short','Lag de archivo de velas respecto de offsets disponibles.'),(16,'Escenario de costo · sin facturación',18,'cryptopulse_estimated_monthly_cost_usd','currencyUSD','Hipótesis editables; no cobro Docker ni cotización. CPU, memoria, disco y horas configurados.')]:
    panel(id,title,'stat',x,79,6,5,expr,unit,desc,prom=True)
panel(17,'Spark · duración de micro-lotes','timeseries',0,84,12,7,'cryptopulse_batch_duration_seconds','s','Comparar duración con intervalo de 15 s. Los lotes de recuperación pueden excederlo.',prom=True)
panel(18,'Kafka · avance acumulado por tópico','timeseries',12,84,12,7,'sum by(topic) (kafka_topic_partition_current_offset{topic=~"market.klines.*"})','short','Suma de offsets. No es tasa de mensajes por segundo ni número retenido.',prom=True)
panel(1,'Histórico limpio · corte batch','stat',0,91,6,5,'SELECT now() AS time,sum(candles)::double precision AS value FROM batch_kpis',description='2022-01 a 2026-09. Archivos públicos con SHA256. Dos registros inválidos excluidos.')
table(35,'Ventanas finalizadas · cobertura real',91,5,"SELECT kind AS \"Ventana\",symbol AS \"Activo\",max(window_end) AS \"Último cierre finalizado\",count(*) AS \"Ventanas\",count(*) FILTER(WHERE candles<>5) AS \"Incompletas\" FROM stream_windows GROUP BY kind,symbol ORDER BY kind,symbol",'Fijas 5 min y deslizantes 5 min/paso 1 min. Watermark 2 min: una ventana finalizada puede estar incompleta; el archivo conserva reparaciones tardías.',x=6,w=18)
dashboard={'uid':'cryptopulse-main','title':'CryptoPulse · BTC y SOL','tags':['cryptopulse','bigdata','risk-monitoring'],'schemaVersion':40,'version':2,'refresh':'10s','timezone':'browser','time':{'from':'now-3h','to':'now'},'editable':True,'graphTooltip':1,'panels':panels,'links':[{'title':'Estado técnico JSON','type':'link','url':'http://localhost:48000/api/status','targetBlank':True},{'title':'Kafka UI','type':'link','url':'http://localhost:48085','targetBlank':True}]}
print(json.dumps(dashboard,ensure_ascii=False,indent=2))

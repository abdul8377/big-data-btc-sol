"""Reglas explicables de vigilancia: observaciones y pronósticos separados.

No crea alertas para llenar un gráfico. Cada evaluación conserva su umbral,
procedencia, vigencia y motivo; una referencia constante no es evidencia de ML.
"""
import math

ASSETS = ("BTCUSDT", "SOLUSDT")


def context_diagnostics(candles):
    """Exige 61 cierres consecutivos para obtener 60 rendimientos completos."""
    result = {}
    for symbol in ASSETS:
        rows = {r["open_ms"]: r for r in candles if r["symbol"] == symbol}
        if not rows:
            result[symbol] = {"feature_ready": False, "missing_minutes": 61,
                              "latest_close_ms": None, "reason": "Sin velas para el activo"}
            continue
        latest = max(rows)
        missing = [latest - i * 60000 for i in range(61) if latest - i * 60000 not in rows]
        result[symbol] = {"feature_ready": not missing, "missing_minutes": len(missing),
                          "latest_close_ms": rows[latest]["close_ms"],
                          "reason": "Contexto completo" if not missing else "Faltan minutos consecutivos; recuperar desde la fuente"}
    return result


def current_pair_anchor(latest):
    """No permite etiquetar un par antiguo como si fuera la última observación."""
    if not all(s in latest for s in ASSETS):
        return None
    btc, sol = (latest[s]["open_ms"] for s in ASSETS)
    return btc if btc == sol else None


def _number(value):
    return value is not None and math.isfinite(float(value))


def build_signals(registry, kpis, predictions):
    """Recibe KPIs y tuplas del sink predictions; devuelve evaluaciones persistibles."""
    thresholds, models = registry["thresholds"], registry["models"]
    version = registry["model_version"]
    signals = []

    def add(dimension, symbol, asof, kind, value, threshold, triggered,
            title, explanation, action, unit, method, mode, horizon=3, available=True):
        finite = _number(value) and _number(threshold)
        available = bool(available and finite)
        triggered = bool(available and triggered)
        if not finite:
            explanation += " No hay datos completos para evaluar esta regla; no equivale a un estado normal."
            action = "Comprobar continuidad y frescura; recuperar velas ausentes desde la fuente."
        signals.append({"dimension": dimension, "symbol": symbol, "as_of_ms": int(asof),
            "kind": kind, "model_version": version, "value": float(value) if _number(value) else None,
            "threshold": float(threshold) if _number(threshold) else None,
            "triggered": triggered, "severity": "high" if triggered and dimension == "crash" else "warning" if triggered else "normal" if available else "unavailable",
            "title": title, "explanation": explanation, "action": action, "unit": unit,
            "method": method, "source_mode": mode, "expires_ms": int(asof + horizon * 60000)})

    for symbol, asof, values in kpis:
        mode = values.get("source_mode", "live")
        if symbol == "BTC-SOL":
            value, threshold = values.get("current_spread15"), thresholds["divergence_alert_pp"]
            add("divergence", symbol, asof, "observed", value, threshold,
                _number(value) and abs(value) >= threshold,
                "Separación reciente entre SOL y BTC",
                "Diferencia de rendimientos de los últimos 15 min: se activa si |SOL menos BTC| alcanza el umbral positivo P95. El valor conserva su signo: positivo, SOL rinde más; negativo, BTC rinde más.",
                "Revisar ambos activos y confirmar si la relación entre sus rendimientos está cambiando.",
                "pp", "historical_p95", mode)
            continue
        reference = thresholds["by_symbol"][symbol]
        value, threshold = values.get("rv15"), reference["volatility_alert_pct"]
        add("volatility", symbol, asof, "observed", value, threshold,
            _number(value) and value >= threshold, "Volatilidad reciente (15 min)",
            "Volatilidad realizada de los últimos 15 min frente al P95 del entrenamiento; no indica dirección del precio.",
            "Elevar la vigilancia de variaciones y revisar la continuidad de los datos.",
            "pct", "historical_p95", mode)
        value, threshold = values.get("r30"), reference["crash_threshold_pct"]
        add("crash", symbol, asof, "observed", value, threshold,
            _number(value) and value <= threshold, "Rendimiento reciente (30 min)",
            "Rendimiento de los últimos 30 min frente al P5 histórico de rendimientos de 30 min; es un hecho observado.",
            "Revisar la magnitud y persistencia de la caída; contrastar con volatilidad y actividad.",
            "pct", "historical_p05", mode)
        ratios = [float(values[k]) / float(values[t]) for k, t in
                  (("quote5", "quote5_p95"), ("trades5", "trades5_p95"))
                  if _number(values.get(k)) and _number(values.get(t)) and values[t] > 0]
        value = max(ratios) if len(ratios) == 2 else None
        explanation = ("Máximo entre volumen USDT/P95 y operaciones/P95 en los últimos 5 min; referencias por activo y hora UTC. "
                       f"Volumen: {values.get('quote5')} USDT / {values.get('quote5_p95')}; operaciones: {values.get('trades5')} / {values.get('trades5_p95')}.")
        add("anomaly", symbol, asof, "observed", value, 1.0,
            _number(value) and value >= 1.0, "Actividad reciente (5 min)", explanation,
            "Examinar el aumento de volumen y operaciones junto con el movimiento del precio.",
            "ratio", "historical_p95_by_hour", mode)

    for p in predictions:
        name, symbol, asof, horizon, forecast, baseline, observed, _, method, mode, _ = p
        item = models[name]
        if name == "volatility":
            threshold, trigger = thresholds["by_symbol"][symbol]["volatility_alert_pct"], forecast >= thresholds["by_symbol"][symbol]["volatility_alert_pct"]
            unit, title = "pct", "Vigilancia de volatilidad a 15 min"
            explanation = "Pronóstico de volatilidad futura frente al P95 histórico; no pronostica dirección ni garantiza el resultado."
        elif name in ("crash", "anomaly"):
            threshold, trigger = item["alert_threshold"], forecast >= item["alert_threshold"]
            unit = "probability"
            title = "Vigilancia de caída a 30 min" if name == "crash" else "Vigilancia de actividad a 5 min"
            explanation = "Probabilidad calibrada frente al umbral seleccionado en validación. Una alerta es una señal para revisar, no certeza de ocurrencia."
            if name=="crash":
                explanation += (f" El evento es un rendimiento al cierre de 30 min menor o igual a {thresholds['by_symbol'][symbol]['crash_threshold_pct']:.3f}%; "
                                f"el umbral de probabilidad de alerta es {100*threshold:.2f}%, no una caída de ese porcentaje.")
            else:
                explanation += " El evento es volumen u operaciones de los próximos 5 min sobre su P95 por activo y hora UTC."
        else:
            threshold, trigger = thresholds["divergence_alert_pp"], abs(forecast) >= thresholds["divergence_alert_pp"]
            unit, title = "pp", "Vigilancia de divergencia a 15 min"
            explanation = "Pronóstico de la diferencia futura SOL menos BTC: se compara su valor absoluto con el umbral positivo en puntos porcentuales; el signo indica qué activo rinde más."
        available = method != "baseline"
        if not available:
            explanation = "Se utiliza una referencia sencilla porque el candidato ML no la mejoró en validación. No se emite alerta predictiva basada en esa referencia; revisar las señales observadas."
        add(name, symbol, asof, "forecast", forecast, threshold, trigger, title, explanation,
            "Contrastar el pronóstico con las señales observadas y revisar el resultado cuando madure su horizonte.",
            unit, method, mode, horizon=horizon, available=available)
    return signals

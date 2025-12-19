require("dotenv").config();
const express = require("express");
const axios = require("axios");

const app = express();
const PORT = process.env.PORT || 3003;

// URLs internas de Docker (nombre del servicio : puerto interno)
const ACQUIRE_URL = process.env.ACQUIRE_URL || "http://acquire:3001/acquire";
const PREDICT_URL = process.env.PREDICT_URL || "http://predict:3002/predict";

app.get("/pipeline", async (req, res) => {
    console.log("[ORCHESTRATOR] Iniciando pipeline...");
    
    try {
        // 1. LLAMAR A ACQUIRE
        console.log(`[ORCHESTRATOR] Solicitando datos a ${ACQUIRE_URL}...`);
        const acquireResp = await axios.get(ACQUIRE_URL);
        const acquireData = acquireResp.data; // { status, db_id, data: {...} }
        
        if (!acquireData.data) {
            throw new Error("Acquire no devolvió datos válidos");
        }

        const d = acquireData.data;
        console.log("[ORCHESTRATOR] Datos recibidos de Acquire:", d);


        const features = [
            d.consumo_hoy,
            d.consumo_ayer,
            d.consumo_antes_ayer,
            d.dia_semana,
            d.mes,
            d.dia_del_mes,
            0 
        ];

        console.log("[ORCHESTRATOR] Features preparadas:", features);

        // 3. LLAMAR A PREDICT
        console.log(`[ORCHESTRATOR] Enviando a Predict ${PREDICT_URL}...`);
        const predictBody = {
            features: features,
            meta: {
                source_db_id: acquireData.db_id, // Guardamos referencia al ID de raw data
                simulation_date: acquireData.simulationDate
            }
        };

        const predictResp = await axios.post(PREDICT_URL, predictBody);
        const predictionData = predictResp.data;

        console.log("[ORCHESTRATOR] Predicción recibida:", predictionData);

        // 4. DEVOLVER RESULTADO FINAL
        res.json({
            status: "success",
            step_1_acquire: acquireData,
            step_2_features: features,
            step_3_predict: predictionData
        });

    } catch (error) {
        console.error("[ORCHESTRATOR] Error:", error.message);
        if (error.response) {
            console.error("Detalles error remoto:", error.response.data);
        }
        res.status(500).json({ 
            error: "Pipeline failed", 
            message: error.message,
            details: error.response ? error.response.data : null
        });
    }
});

app.listen(PORT, () => {
    console.log(`[ORCHESTRATOR] Escuchando en puerto ${PORT}`);
});
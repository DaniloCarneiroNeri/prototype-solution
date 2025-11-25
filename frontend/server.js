// frontend/server.js

import express from "express";
import cors from "cors";
import fs from "fs";
import multer from "multer";
import path from "path";

// ======== CONFIGURAÇÃO DO BACKEND ========
const PYTHON_URL =
  process.env.PYTHON_URL || "http://localhost:8000/upload";

console.log("🚀 PYTHON_URL ativo:", PYTHON_URL);

const app = express();
const upload = multer({ dest: "uploads/" });

app.use(cors());
app.use(express.json());
app.use(express.static("public"));

// ======== ROTA DE UPLOAD ========
app.post("/upload", upload.single("file"), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: "Nenhum arquivo enviado." });
    }

    const fileBuffer = fs.readFileSync(req.file.path);

    // Node 18 → usa Blob nativo
    const blob = new Blob([fileBuffer]);

    // Node 18 → FormData nativo
    const form = new FormData();
    form.append("file", blob, req.file.originalname);

    // ======== ENVIA PARA O BACKEND CORRETO ========
    const response = await fetch(PYTHON_URL, {
      method: "POST",
      body: form
    });

    if (!response.ok) {
      const errText = await response.text();
      console.error("BACKEND ERROR:", errText);
      throw new Error(`Backend retornou status ${response.status}`);
    }

    const json = await response.json();
    fs.unlinkSync(req.file.path);

    return res.json(json);

  } catch (err) {
    console.error("UPLOAD ERROR:", err);
    return res.status(500).json({
      error: "Erro interno no frontend.",
      details: err.message
    });
  }
});

// ======== FRONTEND ROOT ========
app.get("/", (req, res) => {
  res.sendFile(path.join(path.resolve(), "public/index.html"));
});

app.listen(3000, () =>
  console.log("Frontend rodando em http://localhost:3000")
);

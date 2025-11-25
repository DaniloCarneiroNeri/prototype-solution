// frontend/server.js

import express from "express";
import cors from "cors";
import fs from "fs";
import multer from "multer";
import path from "path";

const app = express();
const upload = multer({ dest: "uploads/" });

app.use(cors());
app.use(express.json());
app.use(express.static("public"));

app.post("/upload", upload.single("file"), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: "Nenhum arquivo enviado." });
    }

    const fileBuffer = fs.readFileSync(req.file.path);

    // IMPORTANTÍSSIMO: FormData nativo do Node 18 espera um Blob
    const blob = new Blob([fileBuffer]);

    // FormData nativo do Node 18
    const form = new FormData();
    form.append("file", blob, req.file.originalname);

    const response = await fetch("http://python-backend:8000/upload", {
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

app.get("/", (req, res) => {
  res.sendFile(path.join(path.resolve(), "public/index.html"));
});

app.listen(3000, () =>
  console.log("Frontend rodando em http://localhost:3000")
);

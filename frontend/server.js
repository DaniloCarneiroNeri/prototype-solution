// frontend/server.js

import express from "express";
import cors from "cors";
import fs from "fs";
import multer from "multer";
import path from "path";
import axios from "axios";
import FormData from "form-data";
import { fileURLToPath } from "url";

// ======== FIX PARA __dirname ========
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ======== CONFIGURAÇÃO BACKENDS ========
const PYTHON_URL = process.env.PYTHON_URL || "http://localhost:8000/upload";
const BACKEND_URL = process.env.BACKEND_URL || "http://localhost:8000";

console.log("🚀 PYTHON_URL:", PYTHON_URL);
console.log("🚀 BACKEND_URL:", BACKEND_URL);

const app = express();
const upload = multer({ dest: "uploads/" });

app.use(express.urlencoded({ extended: true }));
app.use(express.json());
app.use(cors());
app.use(express.static(path.join(__dirname, "public")));

// ======================================
// UPLOAD PARA O FASTAPI
// ======================================
app.post("/upload", upload.single("file"), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: "Nenhum arquivo enviado." });

    const fileBuffer = fs.readFileSync(req.file.path);

    const form = new FormData();
    form.append("file", fileBuffer, req.file.originalname);

    const response = await axios.post(PYTHON_URL, form, {
      headers: form.getHeaders()
    });

    fs.unlinkSync(req.file.path);

    return res.json(response.data);

  } catch (err) {
    console.error("UPLOAD ERROR:", err);
    return res.status(500).json({
      error: "Erro interno no frontend.",
      details: err.message
    });
  }
});

// ======================================
// ROTA DE LOGIN (HTML)
// ======================================
app.get("/login", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "login.html"));
});

// ======================================
// POST LOGIN → chama backend FastAPI
// ======================================
app.post("/login", async (req, res) => {
  try {
    const { username, password } = req.body;

    const response = await axios.post(`${BACKEND_URL}/login`, {
      username,
      password,
    });

    return res.json(response.data);

  } catch (error) {
    console.log(error);
    return res.status(401).json({ msg: "Login inválido" });
  }
});

// ======================================
// MIDDLEWARE DE AUTENTICAÇÃO
// ======================================
function authMiddleware(req, res, next) {
  const token = req.headers.authorization || req.query.token;

  if (!token) return res.redirect("/login");
  next();
}

// ======================================
// DASHBOARD (INDEX.HTML)
// ======================================
app.get("/", authMiddleware, (req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

// ======================================
// START SERVER
// ======================================
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Frontend rodando na porta ${PORT}`));

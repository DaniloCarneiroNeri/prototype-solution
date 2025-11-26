// =========================
// Variáveis Globais
// =========================
let globalData = [];
let globalFileName = "resultado";

// =========================
// Controle de Estado da Tabela
// =========================
function setTableLoading(isLoading) {
    const tbl = document.getElementById("dataTable");
    if (isLoading) {
        tbl.classList.add("opacity-50");
        tbl.style.pointerEvents = "none";
    } else {
        tbl.classList.remove("opacity-50");
        tbl.style.pointerEvents = "auto";
    }
}

// =========================
// UPLOAD
// =========================
document.getElementById("btnUpload").onclick = () => {
    const fileInput = document.getElementById("fileInput");
    const file = fileInput.files[0];
    const btn = document.getElementById("btnUpload");

    const progressContainer = document.getElementById("progressContainer");
    const progressBar = document.getElementById("progressBar");
    const progressPercent = document.getElementById("progressPercent");
    const progressStatus = document.getElementById("progressStatus");

    if (!file) {
        alert("Selecione um arquivo Excel primeiro!");
        return;
    }

    globalFileName = file.name.split('.')[0] + "_processado";

    const formData = new FormData();
    formData.append("file", file);

    btn.innerText = "Enviando...";
    btn.disabled = true;
    btn.classList.add("opacity-50");
    document.getElementById("result").classList.add("hidden"); 
    
    progressContainer.classList.remove("hidden");
    progressBar.style.width = "0%";
    progressPercent.innerText = "0%";
    progressStatus.innerText = "Carregando arquivo...";

    const xhr = new XMLHttpRequest();

    // Progresso do upload
    xhr.upload.onprogress = (event) => {
        if (event.lengthComputable) {
            const pct = Math.round((event.loaded / event.total) * 100);
            progressBar.style.width = pct + "%";
            progressPercent.innerText = pct + "%";
            if (pct === 100) {
                progressStatus.innerText = "Processando dados...";
                progressBar.classList.remove("bg-blue-600");
                progressBar.classList.add("bg-green-500", "animate-pulse");
            }
        }
    };

    xhr.onload = () => {
        resetUI();
        if (xhr.status >= 200 && xhr.status < 300) {
            const resp = JSON.parse(xhr.responseText);
            handleSuccess(resp);
        } else {
            alert("Erro ao processar.");
        }
    };

    xhr.onerror = () => {
        resetUI();
        alert("Erro de conexão.");
    };

    xhr.open("POST", "/upload", true);
    xhr.send(formData);

    function resetUI() {
        btn.innerText = "Enviar";
        btn.disabled = false;
        btn.classList.remove("opacity-50");
        setTimeout(() => {
             progressContainer.classList.add("hidden");
             progressBar.classList.add("bg-blue-600");
             progressBar.classList.remove("bg-green-500", "animate-pulse");
        }, 500);
    }

    function handleSuccess(resp) {

    let data = resp.data;

    data = data.map(row => {
    const cleaned = { ...row };
    delete cleaned["Unnamed: 0"];
    return cleaned;
    });

    // remover linhas totalmente vazias
    data = data.filter(row => {
        return Object.values(row).some(v => v !== null && v !== "" && v !== undefined);
    });

    globalData = data;
    document.getElementById("result").classList.remove("hidden");
    document.getElementById("jsonOutput").innerText = JSON.stringify(resp, null, 2);

    const columns = data.length ? Object.keys(data[0]) : [];

    document.getElementById("statTotalLines").innerText = data.length;

    let notFound = data.filter(row => row["Geo_Latitude"] === "Não encontrado").length;

    document.getElementById("statNotFound").innerText = notFound;
    
    let partial = data.filter(r => r["Partial_Match"] === true).length;
    document.getElementById("statPartial").innerText = partial;

    let cond = data.filter(r => r["Cond_Match"] === true).length;
    document.getElementById("statCond").innerText = cond;

    renderTable(columns, data);
}
};

// =========================
// Renderização da Tabela
// =========================
function renderTable(columns, data) {
    setTableLoading(true);

    const tableHead = document.getElementById("tableHead");
    const tableBody = document.getElementById("tableBody");

    tableHead.innerHTML = "";
    tableBody.innerHTML = "";

    const hiddenCols = ["Partial_Match", "Unnamed: 0","Geo_Longitude","Stop","Latitude","Longitude","Cond_Match"];

    const visibleCols = columns.filter(c => !hiddenCols.includes(c));

    // ============================================================
    // Cabeçalho
    // ============================================================
    if (visibleCols.length) {
        const trTitle = document.createElement("tr");
        const trFilter = document.createElement("tr");

        visibleCols.forEach(col => {
            // título
            const th = document.createElement("th");
            th.className = "border-b border-r px-4 py-2 text-left font-bold bg-gray-200";
            th.textContent = col;
            trTitle.appendChild(th);

            // filtro
            const thF = document.createElement("th");
            thF.className = "border-b border-r p-1 bg-gray-100";

            const inp = document.createElement("input");
            inp.type = "text";
            inp.placeholder = "Filtrar...";
            inp.className = "w-full text-xs p-1 border rounded";
            inp.dataset.colname = col;   // <--- essencial p/ filterTable
            inp.onkeyup = () => filterTable();

            thF.appendChild(inp);
            trFilter.appendChild(thF);
        });

        tableHead.appendChild(trTitle);
        tableHead.appendChild(trFilter);
    }

    // ============================================================
    // Corpo da tabela
    // ============================================================
    let index = 0;

    function addNext() {
        if (index >= data.length) {
            setTableLoading(false);
            return;
        }
        addRowFixed(data[index], visibleCols);
        index++;
        requestAnimationFrame(addNext);
    }

    addNext();
}

function addRowFixed(row, visibleCols) {
    const tb = document.getElementById("tableBody");

    const tr = document.createElement("tr");
    tr.className = "odd:bg-white even:bg-slate-50 hover:bg-blue-50";

    const unifiedValue = `${row["Geo_Latitude"] ?? ""}  ${row["Geo_Longitude"] ?? ""}`;

    visibleCols.forEach(col => {
        const td = document.createElement("td");

        // Unifica colunas
        if (col === "Geo_Latitude" || col === "Geo_Longitude") {
            if (col !== "Geo_Latitude") return;
            col = "Geo_Lat_Lng";
        }

        let value =
            col === "Geo_Lat_Lng"
                ? unifiedValue
                : (row[col] ?? "");

        td.className = "border-b border-r px-4 py-2";

        const strValue = String(value);

        const isNotFound = strValue.includes("Não encontrado");
        const isPartial = row["Partial_Match"] === true && col === "Geo_Lat_Lng";
        const isCond = row["Cond_Match"] === true && col === "Geo_Lat_Lng";

        if (isNotFound || isPartial || isCond) {
            td.classList.add("p-0");

            const input = document.createElement("input");
            input.type = "text";
            input.value = (isCond && col === "Geo_Lat_Lng") ? "Condomínio" : strValue;

            input.className = `
                w-full h-full px-2 py-1 outline-none
                ${isNotFound ? "text-red-600 font-bold bg-red-50" : ""}
                ${isPartial ? "bg-yellow-100 font-bold text-yellow-900" : ""}
                ${isCond ? "bg-purple-100 font-bold text-purple-900" : ""}
            `.trim();

            if (isPartial) {
                input.title = "Endereço encontrado parcialmente - VERIFIQUE";
            }

            // Atualiza row ao digitar
            input.addEventListener("input", () => {
                const clean = input.value.trim();
                const parts = clean.split(/\s+/);

                if (parts.length >= 2) {
                    // OK — usuário informou os 2 valores
                    row["Geo_Latitude"] = parts[0];
                    row["Geo_Longitude"] = parts[1];
                } else if (parts.length === 1) {
                    // Só latitude digitada — NÃO apaga longitude
                    row["Geo_Latitude"] = parts[0];
                }

                // Reformatar visualmente enquanto digita
                input.value =
                    `${row["Geo_Latitude"] ?? ""}  ${row["Geo_Longitude"] ?? ""}`;
            });

            td.appendChild(input);
        } else {
            td.textContent = strValue;
        }

        tr.appendChild(td);
    });

    tb.appendChild(tr);
}

// =========================
// Exportação Excel
// =========================
function exportToExcel() {
    if (!globalData.length) return alert("Não há dados!");

    const ws = XLSX.utils.json_to_sheet(globalData);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, "Dados Processados");
    XLSX.writeFile(wb, `${globalFileName}.xlsx`);
}

// =========================
// Exportação Circuit
// =========================
function exportToCircuit() {
    if (!globalData.length) {
        alert("Nenhum dado carregado!");
        return;
    }

    const grouped = new Map(); // chave = "lat|lng"

    globalData.forEach((r, i) => {
        const lat = r["Geo_Latitude"];
        const lng = r["Geo_Longitude"];

        if (lat === "Não encontrado" || lng === "Não encontrado") return;

        const normalized = r["Normalized_Address"] || "";
        let quadra = "";
        let lote = "";

        // ⚠️ Regex CORRIGIDO → só aceita números
        const match = normalized.match(/,\s*([0-9]+)-([0-9]+)/);
        if (match) {
            quadra = match[1];
            lote = match[2];
        }

        const seq = i + 1;
        const key = `${lat}|${lng}`;

        if (grouped.has(key)) {
            const existing = grouped.get(key);

            // 🔥 Adiciona sequência
            existing.Sequencias.push(seq);

            // quadra/lote devem ser iguais — mantém o primeiro
        } else {
            grouped.set(key, {
                Geo_Latitude: lat,
                Geo_Longitude: lng,
                Quadra: quadra,
                Lote: lote,
                Sequencias: [seq]
            });
        }
    });

    const rows = Array.from(grouped.values());

    if (rows.length === 0) {
        alert("Nenhum dado válido encontrado para exportação.");
        return;
    }

    let csv = "Geo_Latitude,Observacoes\n";

    rows.forEach(r => {
        // Junta sequências: 28, 29, 30
        const seqStr = r.Sequencias.join(", ");

        const obs =
            `${seqStr} - Quadra:${r.Quadra} - Lote:${r.Lote}`;

        csv += `${r.Geo_Latitude},"${obs}"\n`;
    });

    const blob = new Blob([csv], { type: "text/csv" });
    const url = URL.createObjectURL(blob);

    const a = document.createElement("a");
    a.href = url;
    a.download = `${globalFileName}_CIRCUIT.csv`;
    a.click();

    URL.revokeObjectURL(url);
}




// =========================
// Popup
// =========================
function openPopup() {
    document.getElementById("exportPopup").classList.remove("hidden");
}

function closePopup() {
    document.getElementById("exportPopup").classList.add("hidden");
}

document.getElementById("popupExportExcel").onclick = () => {
    closePopup();
    exportToExcel();
};

document.getElementById("popupExportCircuit").onclick = () => {
    closePopup();
    exportToCircuit();
};

function openInfoPopup() {
    const popup = document.getElementById("infoPopup");
    popup.classList.remove("hidden");
    popup.classList.add("flex");

    document.body.classList.add("backdrop-blur-sm");
}

function closeInfoPopup() {
    const popup = document.getElementById("infoPopup");
    popup.classList.add("hidden");
    popup.classList.remove("flex");

    document.body.classList.remove("backdrop-blur-sm");
}

// =========================
// Filtro Dinâmico
// =========================
window.filterTable = function () {
    const table = document.getElementById("dataTable");
    const tr = table.querySelector("tbody").getElementsByTagName("tr");
    const inputs = table.querySelector("thead").getElementsByTagName("input");

    const filters = Array.from(inputs).map(i => i.value.toUpperCase());

    for (let row of tr) {
        let show = true;
        let tds = row.getElementsByTagName("td");

        for (let i = 0; i < filters.length; i++) {
            if (filters[i] && tds[i]) {
                const txt = tds[i].textContent.toUpperCase();
                if (!txt.includes(filters[i])) {
                    show = false;
                    break;
                }
            }
        }
        row.style.display = show ? "" : "none";
    }
};
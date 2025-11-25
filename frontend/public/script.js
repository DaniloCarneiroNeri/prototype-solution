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
        globalData = resp.data;

        document.getElementById("result").classList.remove("hidden");
        document.getElementById("jsonOutput").innerText = JSON.stringify(resp, null, 2);

        const data = resp.data;
        const columns = data.length ? Object.keys(data[0]) : [];

        const total = data.length;
        let notFound = 0;

        data.forEach(row => {
            if (Object.values(row).includes("Não encontrado")) notFound++;
        });

        document.getElementById("statTotalLines").innerText = total;
        document.getElementById("statNotFound").innerText = notFound;

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

    // Cabeçalhos
    if (columns.length) {
        const trTitle = document.createElement("tr");
        const trFilter = document.createElement("tr");

        columns.forEach(col => {
            const th = document.createElement("th");
            th.className = "border-b border-r px-4 py-2 text-left font-bold bg-gray-200";
            th.textContent = col;
            trTitle.appendChild(th);

            const thF = document.createElement("th");
            thF.className = "border-b border-r p-1 bg-gray-100";

            const inp = document.createElement("input");
            inp.type = "text";
            inp.placeholder = "Filtrar...";
            inp.className = "w-full text-xs p-1 border rounded";
            inp.onkeyup = () => filterTable();
            thF.appendChild(inp);

            trFilter.appendChild(thF);
        });

        tableHead.appendChild(trTitle);
        tableHead.appendChild(trFilter);
    }

    // Preenchimento em tempo real
    let index = 0;

    function addNext() {
        if (index >= data.length) {
            setTableLoading(false);
            return;
        }
        addRow(data[index], columns);
        index++;
        requestAnimationFrame(addNext);
    }

    addNext();
}

function addRow(row, columns) {
    const tb = document.getElementById("tableBody");

    const tr = document.createElement("tr");
    tr.className = "odd:bg-white even:bg-slate-50 hover:bg-blue-50";

   columns.forEach(col => {
    const td = document.createElement("td");
    const value = row[col] ?? "";

    td.textContent = value;
    td.className = "border-b border-r px-4 py-2";

    // 🔴 Caso 1: Não encontrado → vermelho
    if (value === "Não encontrado") {
        td.classList.add("text-red-600", "font-bold", "bg-red-50");
    }

    // 🟡 Caso 2: Endereço encontrado parcialmente → amarelo
    if (row["Partial_Match"] === true && 
        (col === "Geo_Latitude" || col === "Geo_Longitude")) {

        td.classList.add("bg-yellow-100", "text-yellow-900");
        td.title = "Endereço encontrado parcialmente";
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

    const rows = [];

    globalData.forEach((r, i) => {

        // IGNORA LINHAS INVÁLIDAS
        if (
            r["Geo_Latitude"] === "Não encontrado" ||
            r["Geo_Longitude"] === "Não encontrado"
        ) {
            return;
        }

        const normalized = r["Normalized_Address"] || "";
        let quadra = "";
        let lote = "";

        const match = normalized.match(/,?\s*([0-9A-Z]+)-([0-9A-Z]+)/);
        if (match) {
            quadra = match[1];
            lote = match[2];
        }

        const obs = `${i + 1}-Quadra:${quadra} - Lote:${lote}`;

        rows.push({
            Geo_Latitude: r["Geo_Latitude"],
            Geo_Longitude: r["Geo_Longitude"],
            Observacoes: obs
        });
    });

    if (rows.length === 0) {
        alert("Nenhum dado válido encontrado para exportação.");
        return;
    }

    let csv = "Geo_Latitude,Geo_Longitude,Observacoes\n";
    rows.forEach(r => {
        csv += `${r.Geo_Latitude},${r.Geo_Longitude},"${r.Observacoes}"\n`;
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

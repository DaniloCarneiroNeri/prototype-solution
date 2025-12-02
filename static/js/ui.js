// ============================================================
// UI.JS - Responsável apenas por manipular o HTML/DOM
// ============================================================

export function setTableLoading(isLoading) {
    const tbl = document.getElementById("dataTable");
    if (isLoading) {
        tbl.classList.add("opacity-50");
        tbl.style.pointerEvents = "none";
    } else {
        tbl.classList.remove("opacity-50");
        tbl.style.pointerEvents = "auto";
    }
}

export function toggleUploadState(isUploading) {
    const btn = document.getElementById("btnUpload");
    const progressContainer = document.getElementById("progressContainer");
    const progressBar = document.getElementById("progressBar");
    const resultDiv = document.getElementById("result");

    if (isUploading) {
        btn.innerText = "Enviando...";
        btn.disabled = true;
        btn.classList.add("opacity-50");
        resultDiv.classList.add("hidden");
        progressContainer.classList.remove("hidden");
        progressBar.style.width = "0%";
        progressBar.classList.add("bg-blue-600");
        progressBar.classList.remove("bg-green-500", "animate-pulse");
    } else {
        btn.innerText = "Enviar";
        btn.disabled = false;
        btn.classList.remove("opacity-50");
        
        setTimeout(() => {
            progressContainer.classList.add("hidden");
        }, 500);
    }
}

export function updateProgress(pct, statusText) {
    const progressBar = document.getElementById("progressBar");
    const progressPercent = document.getElementById("progressPercent");
    const progressStatus = document.getElementById("progressStatus");

    progressBar.style.width = pct + "%";
    progressPercent.innerText = pct + "%";
    if (statusText) progressStatus.innerText = statusText;

    if (pct === 100) {
        progressBar.classList.remove("bg-blue-600");
        progressBar.classList.add("bg-green-500", "animate-pulse");
    }
}

export function updateStats(data) {
    document.getElementById("statTotalLines").innerText = data.length;
    document.getElementById("statNotFound").innerText = data.filter(row => row["Geo_Latitude"] === "Não encontrado").length;
    document.getElementById("statPartial").innerText = data.filter(r => r["Partial_Match"] === true).length;
    document.getElementById("statCond").innerText = data.filter(r => r["Status_Log"] === "CONDOMINIO_DETECTED").length;
}

export function updateTitle(data) {
    if (!data || !data.length) return;
    const firstRow = data[0];
    const firstKey = Object.keys(firstRow)[0];
    const firstValue = firstRow[firstKey];
    document.getElementById("resultTitle").textContent = firstValue || "Arquivo carregado";
}

// =========================
// Lógica da Tabela
// =========================
export function renderTable(columns, data) {
    columns = columns.filter(c => !["AT ID", "Stop", "SPX TN", "Latitude", "Longitude", "idx", "Partial_Match", "Cond_Match","Status_Log"].includes(c));
    const table = document.getElementById("dataTable");
    const thead = table.querySelector("thead");
    const tbody = document.getElementById("tableBody");
    
    // 1. Limpa tudo
    thead.innerHTML = "";
    tbody.innerHTML = "";

    if (!data || data.length === 0) return;

    // ---------------------------------------------------------
    // A. CRIAÇÃO DO CABEÇALHO (Dinâmico com Filtros)
    // ---------------------------------------------------------
    const trHead = document.createElement("tr");
    trHead.className = "bg-gray-100 text-gray-700 uppercase font-bold text-sm";

    const allHeaders = ["Ação", "Status", ...columns];

    allHeaders.forEach((colName, index) => {
        const th = document.createElement("th");
        th.className = "px-4 py-3 border-b border-gray-300 min-w-[150px]";
        
        const titleDiv = document.createElement("div");
        titleDiv.textContent = colName;
        titleDiv.className = "mb-2";
        th.appendChild(titleDiv);

        const input = document.createElement("input");
        input.type = "text";
        input.placeholder = "Filtrar...";
        input.className = "w-full px-2 py-1 text-xs font-normal border rounded focus:outline-none focus:border-blue-500 text-gray-600";
        
        input.addEventListener("keyup", filterTableLogic);

        if (index === 0) {
            input.disabled = true;
            input.placeholder = "";
            input.classList.add("bg-transparent", "border-0");
        }

        th.appendChild(input);
        trHead.appendChild(th);
    });

    thead.appendChild(trHead);

    // ---------------------------------------------------------
    // B. CRIAÇÃO DO CORPO (Dados)
    // ---------------------------------------------------------
    data.forEach((row, index) => {
        const tr = document.createElement("tr");
        tr.className = "border-b hover:bg-gray-50 transition duration-150";

        // --- Lógica de Status (Copiada do seu código anterior) ---
        const lat = row["Geo_Latitude"];
        const isFound = lat && lat !== "Não encontrado";
        const isPartial = row["Partial_Match"] === true;
        const isCond = row["Status_Log"] === "CONDOMINIO_DETECTED";
        const statusLog = row["Status_Log"];

        let statusBadge = "";
        let needsFix = false;

        if (!isFound && !isCond) {
            let tooltipText = "Endereço Incorreto ou não encontrado."; 
            statusBadge = `
            <div class="group relative inline-flex flex-col items-center cursor-help">
                <span class="bg-red-100 text-red-800 text-xs font-bold px-2 py-0.5 rounded border border-red-200">
                    Erro
                </span>
                
                <div class="invisible opacity-0 group-hover:visible group-hover:opacity-100 transition-all duration-200 ease-in-out absolute bottom-full mb-2 w-48 p-2 bg-gray-800 text-white text-xs text-center rounded shadow-lg z-50 pointer-events-none transform group-hover:-translate-y-1">
                    ${tooltipText}
                    <div class="absolute top-full left-1/2 transform -translate-x-1/2 border-4 border-transparent border-t-gray-800"></div>
                </div>
            </div>`;
            
            needsFix = true;

        } else if (isPartial) {
            let tooltipText = "Atenção necessária no endereço."; 

            if (statusLog === "BAIRRO_MISMATCH") {
                tooltipText = "Endereço encontrado parcialmente: Bairro divergente.";
            } else if (statusLog === "CONDOMINIO_DETECTED") {
                tooltipText = "Endereço identificado como condomínio.";
            } else if (statusLog) {
                tooltipText = `Divergência: ${statusLog}`;
            }

            statusBadge = `
            <div class="group relative inline-flex flex-col items-center cursor-help">
                <span class="bg-yellow-100 text-yellow-800 text-xs font-bold px-2 py-0.5 rounded border border-yellow-200">
                    Parcial
                </span>
                
                <div class="invisible opacity-0 group-hover:visible group-hover:opacity-100 transition-all duration-200 ease-in-out absolute bottom-full mb-2 w-48 p-2 bg-gray-800 text-white text-xs text-center rounded shadow-lg z-50 pointer-events-none transform group-hover:-translate-y-1">
                    ${tooltipText}
                    <div class="absolute top-full left-1/2 transform -translate-x-1/2 border-4 border-transparent border-t-gray-800"></div>
                </div>
            </div>`;
            
            needsFix = true;

        } else if (isCond) {
            let tooltipText = "Endereço Identificado como Condimínio."; 

            statusBadge = `
            <div class="group relative inline-flex flex-col items-center cursor-help">
                <span class="bg-purple-100 text-purple-800 text-xs font-bold px-2 py-0.5 rounded border border-purple-200">
                    Condomínio
                </span>
                
                <div class="invisible opacity-0 group-hover:visible group-hover:opacity-100 transition-all duration-200 ease-in-out absolute bottom-full mb-2 w-48 p-2 bg-gray-800 text-white text-xs text-center rounded shadow-lg z-50 pointer-events-none transform group-hover:-translate-y-1">
                    ${tooltipText}
                    <div class="absolute top-full left-1/2 transform -translate-x-1/2 border-4 border-transparent border-t-gray-800"></div>
                </div>
            </div>`;
            
            needsFix = true;

        } else {
            statusBadge = `<span class="bg-green-100 text-green-800 text-xs font-bold px-2 py-0.5 rounded border border-green-200">OK</span>`;
        }
        if (row["Status_Log"] === "MANUAL_FIX") {
            statusBadge = `<span class="bg-blue-100 text-blue-800 text-xs font-bold px-2 py-0.5 rounded border border-blue-200">Manual</span>`;
            needsFix = false; 
        }
        // --- Coluna 1: Ação (Botão Mapa) ---
        const tdAction = document.createElement("td");
        tdAction.className = "px-4 py-3 text-center border-r";
        
        if (needsFix) {
            tdAction.innerHTML = `
                <div class="flex justify-center items-center gap-2">
                <button onclick="window.deleteRow(${index})" 
                    class="bg-red-50 hover:bg-red-600 hover:text-white text-red-600 rounded-full w-8 h-8 flex items-center justify-center transition shadow-sm border border-red-200" 
                    title="Excluir Linha">
                    <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                        <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
                    </svg>
                </button>

                <button onclick="window.openEditor(${index})" 
                    class="bg-blue-50 hover:bg-blue-600 hover:text-white text-blue-600 rounded-full w-8 h-8 flex items-center justify-center transition shadow-sm border border-blue-200" 
                    title="Corrigir no Mapa">
                    ➤
                </button>`;
        } else {
            tdAction.innerHTML = `<span class="text-green-500 font-bold text-xl">✓</span>`;
        }
        tr.appendChild(tdAction);

        // --- Coluna 2: Status ---
        const tdStatus = document.createElement("td");
        tdStatus.className = "px-4 py-3 text-center border-r overflow-visible";
        tdStatus.innerHTML = statusBadge;
        
        tdStatus.setAttribute("data-search", row["Status_Log"] || (isFound ? "OK" : "Erro")); 
        tr.appendChild(tdStatus);

        // --- Colunas 3...N: Dados Originais do Excel ---
        columns.forEach(col => {
            const td = document.createElement("td");
            td.className = "px-4 py-3 text-sm text-gray-700 border-r whitespace-nowrap max-w-xs overflow-hidden text-ellipsis";
            
            let value = row[col];
            if (value === null || value === undefined) value = "";
            
            td.textContent = value;
            td.title = value;
            tr.appendChild(td);
        });

        tbody.appendChild(tr);
    });
}

// =========================
// Lógica de Filtro (Ajustada)
// =========================
function filterTableLogic() {
    const table = document.getElementById("dataTable");
    const tbody = table.querySelector("tbody");
    const trs = tbody.getElementsByTagName("tr");
    
    const inputs = table.querySelector("thead").getElementsByTagName("input");
    
    const filters = Array.from(inputs).map(i => i.value.toUpperCase().trim());

    for (let row of trs) {
        let show = true;
        const tds = row.getElementsByTagName("td");

        for (let i = 0; i < filters.length; i++) {
            if (!filters[i]) continue;
        
            if (!tds[i]) continue;

            const cellText = tds[i].innerText || tds[i].getAttribute("data-search") || "";
            
            if (!cellText.toUpperCase().includes(filters[i])) {
                show = false;
                break; 
            }
        }
        row.style.display = show ? "" : "none";
    }
}

// =========================
// Temas e Popups
// =========================
export function setupTheme() {
    const html = document.documentElement;
    const btn = document.getElementById("toggleTheme");
    const icon = document.getElementById("themeIcon");

    function setIcon() {
        const isDark = html.classList.contains("dark");
        icon.innerHTML = isDark 
            ? `<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 3v1m0 16v1m8-9h1M3 12H2m15.364-6.364l.707.707M6.343 17.657l-.707.707m12.728 0l-.707-.707M6.343 6.343l-.707-.707" />`
            : `<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 12.79A9 9 0 1111.21 3 7 7 0 0021 12.79z"/>`;
    }

    if (localStorage.getItem("theme") === "dark") html.classList.add("dark");
    setIcon();

    btn.addEventListener("click", () => {
        html.classList.toggle("dark");
        localStorage.setItem("theme", html.classList.contains("dark") ? "dark" : "light");
        setIcon();
    });
}

export function setupPopups() {
    const toggle = (popupId, show) => {
        const popup = document.getElementById(popupId);
        if (!popup) return;

        if (show) {
            popup.classList.remove("hidden");
            popup.classList.add("flex");
            if (popupId === "infoPopup" || popupId === "exportPopup") {
                document.body.classList.add("backdrop-blur-sm");
            }
        } else {
            popup.classList.add("hidden");
            popup.classList.remove("flex");
            document.body.classList.remove("backdrop-blur-sm");
        }
    };

    const bindPopup = (triggerId, popupId, closeSelector) => {
        const trigger = document.getElementById(triggerId);
        const popup = document.getElementById(popupId);

        if (!popup) return;

        if (trigger) {
            trigger.onclick = (e) => {
                e.preventDefault();
                toggle(popupId, true);
            };
        }

        if (closeSelector) {
            const closeBtn = popup.querySelector(closeSelector);
            if (closeBtn) {
                closeBtn.onclick = (e) => {
                    e.preventDefault();
                    e.stopPropagation();
                    toggle(popupId, false);
                };
            }
        }
        
        const closeButtons = popup.querySelectorAll("button");
        closeButtons.forEach(btn => {
            if (
                btn.innerText.includes("Cancelar") || 
                btn.innerText.includes("Fechar") || 
                btn.innerText === "×" ||
                btn.getAttribute("onclick")?.includes("close") 
            ) {
                btn.onclick = (e) => {
                    e.preventDefault(); 
                    e.stopPropagation();
                    toggle(popupId, false);
                };
            }
        });
        
        popup.onclick = (e) => {
            if (e.target === popup) {
                toggle(popupId, false);
            }
        };
    };

    bindPopup("btnExport", "exportPopup", null); 
    bindPopup("helpButton", "helpPopup", "button.absolute"); 


    const footer = document.querySelector("footer");
    if (footer) {
        footer.onclick = () => toggle("infoPopup", true);
    }

    bindPopup(null, "infoPopup", "button.absolute");
}
// ============================================================
// EXPORTS.JS - Lógica de geração de arquivos
// ============================================================

export function exportToExcel(data, fileName, tableId = "dataTable") {
    if (!data || !data.length) return alert("Não há dados para exportar!");
    
    // Verifica biblioteca
    if (typeof XLSX === 'undefined') return alert("Biblioteca XLSX não carregada.");

    // 1. Tenta pegar a tabela HTML pelo SEU ID existente
    const table = document.getElementById(tableId);
    let dataToExport = data;

    if (table) {
        const tbody = table.querySelector("tbody");
        // Pega apenas as linhas do corpo da tabela
        const rows = tbody ? Array.from(tbody.querySelectorAll("tr")) : [];

        // Só aplica o filtro se a quantidade de linhas bater com os dados
        // (Isso evita erros de sincronia)
        if (rows.length === data.length) {
            
            dataToExport = data.filter((item, index) => {
                const row = rows[index];
                
                // Verifica se a linha está oculta no CSS
                const isHidden = row.style.display === "none" || row.classList.contains("hidden");
                
                // Se NÃO estiver oculta, incluímos no Excel
                return !isHidden;
            });
        }
    }

    if (dataToExport.length === 0) {
        return alert("Nenhum dado visível para exportar.");
    }

    console.log(`Exportando ${dataToExport.length} linhas.`); // Log para conferência

    // 2. Gera o Excel
    const ws = XLSX.utils.json_to_sheet(dataToExport);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, "Dados");
    
    XLSX.writeFile(wb, `${fileName}.xlsx`);
}

export function exportToCircuit(data, fileName) {
    if (!data || !data.length) return alert("Nenhum dado carregado!");

    const grouped = new Map();

    data.forEach((r, i) => {
        const lat = r["Geo_Latitude"];
        const lng = r["Geo_Longitude"];

        if (lat === "Não encontrado" || lng === "Não encontrado") return;
        if (!lat || !lng) return;

        const normalized = r["Normalized_Address"] || "";
        let quadra = "";
        let lote = "";

        const match = normalized.match(/,\s*([0-9]+)-([0-9]+)/);
        if (match) {
            quadra = match[1];
            lote = match[2];
        }

        const seq = i + 1;
        const key = `${lat}|${lng}`;

        if (grouped.has(key)) {
            const existing = grouped.get(key);
            existing.Sequencias.push(seq);
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
        const seqStr = r.Sequencias.join(", ");
        const obs = `${seqStr} - Quadra:${r.Quadra} - Lote:${r.Lote}`;
        csv += `${r.Geo_Latitude}, ${r.Geo_Longitude},"${obs}"\n`;
    });

    const blob = new Blob([csv], { type: "text/csv" });
    const url = URL.createObjectURL(blob);

    const a = document.createElement("a");
    a.href = url;
    a.download = `${fileName}_CIRCUIT.csv`;
    a.click();

    URL.revokeObjectURL(url);
}
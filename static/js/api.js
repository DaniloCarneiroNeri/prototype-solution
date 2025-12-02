export async function uploadFile(file, onProgress) {
    return new Promise((resolve, reject) => {
        const formData = new FormData();
        formData.append("file", file);

        const xhr = new XMLHttpRequest();
        
        xhr.upload.onprogress = (event) => {
            if (event.lengthComputable && onProgress) {
                const pct = Math.round((event.loaded / event.total) * 100);
                onProgress(pct);
            }
        };

        xhr.onload = () => {
            if (xhr.status >= 200 && xhr.status < 300) {
                try {
                    const resp = JSON.parse(xhr.responseText);
                    resolve(resp);
                } catch (e) {
                    reject("Erro ao ler JSON: " + e);
                }
            } else {
                reject("Erro no servidor: " + xhr.statusText);
            }
        };

        xhr.onerror = () => reject("Erro de conexão");
        xhr.open("POST", "/upload", true); // Rota do FastAPI
        xhr.send(formData);
    });
}
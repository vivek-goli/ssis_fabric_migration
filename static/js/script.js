document.getElementById('migrationForm').onsubmit = async function (event) {
    event.preventDefault();
    const formData = new FormData(this);
    console.log(formData)
    try {
        const response = await fetch('/migrate', {
            method: 'POST',
            body: formData
        });

        const result = await response.json();
        document.getElementById('result').innerHTML = `<p class="success">${result.message}</p>`;
    } catch (error) {
        document.getElementById('result').innerHTML = `<p class="error">Failed to migrate: ${error.message}</p>`;
    }
};

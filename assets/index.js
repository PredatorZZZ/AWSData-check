async function getAndParseFile(fileName, container) {
    var csvData = await fetch(fileName).then((data) => {
        return data.text();
    })
    processData(csvData, container);
}


function getLineValues(str){
    var values = [];
    var ind = str.indexOf(",");
    do {
        var sub = str.substr(0, ind);
        if((sub.match(/"/g) || []).length%2){
            ind = str.indexOf(",", ind+1);
            continue;
        }
        values.push(str.substr(0, ind).replace(/"/g, ""));
        str = str.substr(ind+1);
        ind = str.indexOf(",");
        if(ind == -1){
            ind = str.length;
        }
    } while (str);
    return values;
}


function parseIntoTable(headers, data, container){
    var tableHeaders = headers.map((name) => { return { id: name, header: [{ text: name }] }; });

    var rowHeight = 40;
    new dhx.Grid(container, {
        autoWidth: true,
        adjust: true,
        headerRowHeight: rowHeight,
        rowHeight: rowHeight, 
        height: rowHeight * (data.length+2), 
        columns: tableHeaders,
        data: data
    });
}


function processData(allText, container) {
    var allTextLines = allText.split(/\r\n|\n/); // allText.split("\n");
    var headers = allTextLines[0].split(",");
    var lines = [];
    for (var i = 1; i < allTextLines.length; i++) {
        var data = getLineValues(allTextLines[i]);
        if(data.length == headers.length){
            var tarr = {};
            for (var j = 0; j < headers.length; j++) {
                tarr[headers[j]] = data[j];
            }
            lines.push(tarr);
        }
    }
    console.log(lines)
    parseIntoTable(headers, lines, container)

}


document.addEventListener("DOMContentLoaded", function(){
    var fileInputs = document.querySelectorAll("[filename]");
    for(var i = 0; i < fileInputs.length; i++){
        var container = fileInputs[i];
        var name = container.getAttribute("filename");

        getAndParseFile(name, container);
    }
})

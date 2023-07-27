require('dotenv').config();

(function() {
    // Create the connector object
    let myConnector = tableau.makeConnector();

    // Define the schema
    myConnector.getSchema = function(schemaCallback) {

        //BOARD SCHEMA
        let boards_cols = [
            {id: "id", alias: "BoardID", dataType: tableau.dataTypeEnum.string}, 
            {id: "name", alias: "Board Name", dataType: tableau.dataTypeEnum.string}, 
            {id: "board_folder_id", alias: "Board FolderID", dataType: tableau.dataTypeEnum.string}, 
            {id: "state", alias: "Board State", dataType: tableau.dataTypeEnum.string}
        ];

        let boardsTableSchema = {
            id: "MondayBoards",
            alias: "MondayBoardsTable",
            columns: boards_cols
        };

        //COLUMNS SCHEMA
        let columns_cols = [
            {id: "id", alias: "Column ID", dataType: tableau.dataTypeEnum.string}, 
            {id: "title", alias: "Column Title", dataType: tableau.dataTypeEnum.string}, 
            {id: "type", alias: "Column Type", dataType: tableau.dataTypeEnum.string},
            {id: "settings_str", alias: "Column settings_str", dataType: tableau.dataTypeEnum.string},
            {id: "board_id", alias: "Column BoardID", dataType: tableau.dataTypeEnum.string}  
        ];

        let columnsTableSchema = {
            id: "MondayColumns",
            alias: "MondayColumnsTable",
            columns: columns_cols
        };
        
        //GROUPS SCHEMA
        let groups_cols = [
            {id: "id", alias: "GroupID", dataType: tableau.dataTypeEnum.string}, 
            {id: "title", alias: "Group Title", dataType: tableau.dataTypeEnum.string}, 
            {id: "deleted", alias: "Group isDeleted", dataType: tableau.dataTypeEnum.string},
            {id: "board_id", alias: "GroupsBoardID", dataType: tableau.dataTypeEnum.string} 
        ];

        let groupsTableSchema = {
            id: "MondayGroups",
            alias: "MondayGroupsTable",
            columns: groups_cols
        };

        //ITEMS SCHEMA
        let items_cols = [
            {id: "id", alias: "Item ID", dataType: tableau.dataTypeEnum.string}, 
            {id: "name", alias: "Item Name", dataType: tableau.dataTypeEnum.string}, 
            {id: "state", alias: "Item State", dataType: tableau.dataTypeEnum.string},
            {id: "board_id", alias: "Item BoardID", dataType: tableau.dataTypeEnum.string},
            {id: "group_id", alias: "Item GroupID", dataType: tableau.dataTypeEnum.string} 
        ];

        let itemsTableSchema = {
            id: "MondayItems",
            alias: "MondayItemsTable",
            columns: items_cols
        };

        //ITEMS COLUMN VALUES SCHEMA
        let item_colvals = [
            {id: "id", alias: "Column Value ID", dataType: tableau.dataTypeEnum.string}, 
            {id: "text", alias: "Column Value Text", dataType: tableau.dataTypeEnum.string}, 
            {id: "title", alias: "Column Value Title", dataType: tableau.dataTypeEnum.string},
            {id: "type", alias: "Column Value Type", dataType: tableau.dataTypeEnum.string},
            {id: "value", alias: "Column Value Value", dataType: tableau.dataTypeEnum.string},
            {id: "item_id", alias: "Column Value Item ID", dataType: tableau.dataTypeEnum.string},
            {id: "board_id", alias: "Column Value Board ID", dataType: tableau.dataTypeEnum.string} 
        ];

        let colvalsTableSchema = {
            id: "MondayColumnValues",
            alias: "MondayColumnValuesTable",
            columns: item_colvals
        };

        schemaCallback([
            boardsTableSchema, 
            groupsTableSchema, 
            columnsTableSchema, 
            itemsTableSchema,
            colvalsTableSchema
        ]);
    };

    // Download the data

    myConnector.getData = async function(table, doneCallback) {
        const workspace_id = process.env.WORKSPACEID
        let board_page = 1;
        let item_board_page = 1;
        let item_page = 1;
        let tableData = [];
        let moreBoardPages = true;
        let moreItemPages = true;


        while (moreBoardPages) {
            let itemQuery = `{
                complexity {
                    before
                    query
                    after
                }
                boards (workspace_ids:${workspace_id}, limit:50, page:${board_page}) {
                    id
                    name
                    board_folder_id
                    state
                    columns{
                        id
                        settings_str
                        title
                        description
                        type
                    }
                    groups {
                        id
                        title
                        deleted
                    }
                }
            }`;                        
                        
         
            await fetch ("https://api.monday.com/v2", {
                method: 'post',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization' : process.env.API_KEY,
                },
                body: JSON.stringify({
                    'query' : itemQuery
                })
            })
            .then(response => {
                return response.json();
            })
            .then(resp => {
                try {
                    let data = resp.data;
                    let complexity = data.complexity;
                    console.log(complexity)
                    console.log(resp);
                    console.log(board_page);
                    if (!data.boards || data.boards.length === 0) {
                        moreBoardPages = false;
                    } 

                    // Iterate over the JSON object

                    // GET BOARD DATA
                    if (data.boards) {
                        if (table.tableInfo.id == "MondayBoards") {
                            for (let i = 0, boardLen = data.boards.length; i < boardLen; i++) {
                                tableData.push({
                                    "id": data.boards[i].id,
                                    "name": data.boards[i].name,
                                    "board_folder_id": data.boards[i].board_folder_id,
                                    "state": data.boards[i].state
                                });
                            }
                        }

                        // GET GROUP DATA
                        if (table.tableInfo.id == "MondayGroups") {
                            for (let i = 0, boardLen = data.boards.length; i < boardLen; i++) {
                                for (let j = 0, groupsLen = data.boards[i].groups.length; j < groupsLen; j++) {
                                    tableData.push({
                                        "id": data.boards[i].groups[j].id,
                                        "title": data.boards[i].groups[j].title,
                                        "deleted": data.boards[i].groups[j].deleted,
                                        "board_id": data.boards[i].id
                                    });
                                }
                            }   
                        }    
                        
                        // GET COLUMN DATA
                        if (table.tableInfo.id == "MondayColumns") {
                            for (let i = 0, boardLen = data.boards.length; i < boardLen; i++) {
                                for (let j = 0, columnsLen = data.boards[i].columns.length; j < columnsLen; j++) {
                                    tableData.push({
                                        "id": data.boards[i].columns[j].id,
                                        "title": data.boards[i].columns[j].title,
                                        "type": data.boards[i].columns[j].type,
                                        "settings_str": data.boards[i].columns[j].settings_str,
                                        "board_id": data.boards[i].id
                                    });
                                }
                            }   
                        } 
                    }

                    if (data.boards && data.boards.length > 0) {
                        board_page++;
                    } 
                }
                catch (err) {
                    console.log(err);
                    console.log(resp);
                }
            });
        }

        while (moreItemPages) {
            let itemQuery = `{
                complexity {
                    before
                    query
                    after
                }
                boards (workspace_ids:${workspace_id}, limit:1, page:${item_board_page}) {
                    id
                    name
                    items (limit:500, page:${item_page}) {
                        id
                        name
                        state
                        group {
                            id
                        }
                        column_values {
                            id
                            text
                            title
                            type
                            value
                        }

                    }
                }
            }`;                        
                        
         
            await fetch ("https://api.monday.com/v2", {
                method: 'post',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization' : process.env.API_KEY,
                },
                body: JSON.stringify({
                    'query' : itemQuery
                })
            })
            .then(response => {
                return response.json();
            })
            .then(resp => {
                try {
                    let data = resp.data;
                    let complexity = data.complexity;
                    console.log(complexity)
                    console.log(resp);
                    console.log(item_board_page);
                    console.log(item_page);
                    if (!data.boards || data.boards.length === 0) {
                        moreItemPages = false;
                    } 

                    // Iterate over the JSON object

                    // GET ITEM DATA
                    if (data.boards && data.boards[0].items) {
                        if (table.tableInfo.id == "MondayItems") {
                            for (let i = 0, boardLen = data.boards.length; i < boardLen; i++) {
                                for (let j = 0, itemsLen = data.boards[i].items.length; j < itemsLen; j++) {
                                    tableData.push({
                                        "id": data.boards[i].items[j].id
                                        ,"name": data.boards[i].items[j].name
                                        ,"state": data.boards[i].items[j].state
                                        ,"board_id": data.boards[i].id
                                        ,"group_id": data.boards[i].items[j].group.id
                                    });
                                }
                            }   
                        }  
                    
                    // GET COLUMN VALUES DATA
                        if (table.tableInfo.id == "MondayColumnValues") {
                            for (let i = 0, boardLen = data.boards.length; i < boardLen; i++) {
                                for (let j = 0, itemsLen = data.boards[i].items.length; j < itemsLen; j++) {
                                    for (let k = 0, valuesLen = data.boards[i].items[j].column_values.length; k < valuesLen; k++) {    
                                        tableData.push({
                                            "id": data.boards[i].items[j].column_values[k].id
                                            ,"text": data.boards[i].items[j].column_values[k].text
                                            ,"title": data.boards[i].items[j].column_values[k].title
                                            ,"type": data.boards[i].items[j].column_values[k].type
                                            ,"value": data.boards[i].items[j].column_values[k].value
                                            ,"item_id": data.boards[i].items[j].id
                                            ,"board_id": data.boards[i].id
                                        });
                                    }
                                }
                            }   
                        } 
                    }

                    if (data.boards[0].items && data.boards[0].items.length > 0) {
                        item_page++;  
                    }
                    else if (data.boards && data.boards.length > 0) {
                        item_board_page++;
                        item_page = 1;
                    } 
                }
                catch (err) {
                    console.log(err);
                    console.log(resp);
                }
            });
        }
        table.appendRows(tableData);
        doneCallback();;
    };



    tableau.registerConnector(myConnector);

    // Create event listeners for when the user submits the form
    $(document).ready(function() {
        $("#submitButton").click(function() {
            tableau.connectionName = "Monday.com Feed"; // This will be the data source name in Tableau
            tableau.submit(); // This sends the connector object to Tableau
        });
    });
})();

                        
                        
                        

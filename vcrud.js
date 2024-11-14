const mysql = require("mysql2/promise");

class Vcrud {
    constructor(dbUser, dbPass, dbHost, dbName) {
        this.maxRows = 20000;
        this.connect(dbUser, dbPass, dbHost, dbName);
    }

    async connect(dbUser, dbPass, dbHost, dbName) {
        this.connection = await mysql.createPool({
            host: dbHost,
            user: dbUser,
            password: dbPass,
            database: dbName,
            waitForConnections: true,
            connectionLimit: 10,
            queueLimit: 0,
        });
    }

    conditionsToStrings(conditions) {
        // Converts conditions to SQL-friendly strings
        return conditions.map(([column, operator, value]) => {
            const conditionValue =
                operator.toLowerCase() === "like" ? `%${value}%` : value;
            return `${column} ${operator} ?`;
        });
    }

    async create(table, fields) {
        const columns = Object.keys(fields);
        const placeholders = columns.map(() => "?").join(", ");
        const sql = `INSERT INTO \`${table}\` (${columns.join(
            ", "
        )}) VALUES (${placeholders})`;
        const [result] = await this.connection.execute(
            sql,
            Object.values(fields)
        );
        return result.insertId;
    }

    async read(table, conditions, orOperand = false, orderBy = null) {
        const conditionStrings = this.conditionsToStrings(conditions);
        const logicalOperator = orOperand ? " OR " : " AND ";
        let sql = `SELECT * FROM \`${table}\` WHERE (${conditionStrings.join(
            logicalOperator
        )})`;

        if (orderBy) {
            sql += ` ORDER BY ${orderBy}`;
        }

        sql += ` LIMIT ${this.maxRows}`;
        const values = conditions.map(([_, __, value]) => value);
        const [rows] = await this.connection.execute(sql, values);
        return rows;
    }

    async update(table, fields, conditions, orOperand = false) {
        const setFields = Object.keys(fields)
            .map((column) => `\`${column}\` = ?`)
            .join(", ");
        const conditionStrings = this.conditionsToStrings(conditions);
        const logicalOperator = orOperand ? " OR " : " AND ";

        const sql = `UPDATE \`${table}\` SET ${setFields} WHERE (${conditionStrings.join(
            logicalOperator
        )}) LIMIT ${this.maxRows}`;
        const values = [
            ...Object.values(fields),
            ...conditions.map(([_, __, value]) => value),
        ];
        await this.connection.execute(sql, values);
    }

    async delete(table, conditions, orOperand = false) {
        const conditionStrings = this.conditionsToStrings(conditions);
        const logicalOperator = orOperand ? " OR " : " AND ";

        const sql = `DELETE FROM \`${table}\` WHERE (${conditionStrings.join(
            logicalOperator
        )}) LIMIT ${this.maxRows}`;
        const values = conditions.map(([_, __, value]) => value);
        await this.connection.execute(sql, values);
    }

    async get(table, conditions, orOperand = false, orderBy = null) {
        // Wrapper for read function
        return this.read(table, conditions, orOperand, orderBy);
    }

    async post(table, fields) {
        // Wrapper for create function
        return this.create(table, fields);
    }

    async put(table, fields, conditions, orOperand = false) {
        // Wrapper for update function
        return this.update(table, fields, conditions, orOperand);
    }

    async close() {
        // Closes the database connection pool
        await this.connection.end();
    }
}

module.exports = Vcrud;

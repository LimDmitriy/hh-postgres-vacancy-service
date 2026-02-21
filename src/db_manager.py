import os

import psycopg2
from dotenv import load_dotenv
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


class DBManager:
    def __init__(self):
        load_dotenv()  # Загружаем переменные из .env

        self.dbname = os.getenv("DB_NAME")
        self.user = os.getenv("DB_USER")
        self.password = os.getenv("DB_PASSWORD")
        self.host = os.getenv("DB_HOST")
        self.port = os.getenv("DB_PORT")

        try:
            self.conn = self.connect()
        except psycopg2.OperationalError:
            self.create_database()
            self.conn = self.connect()

    def create_database(self):
        """Создание базы данных"""
        conn = psycopg2.connect(
            dbname="postgres", user=self.user, password=self.password, host=self.host, port=self.port
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        cur.execute(f"CREATE DATABASE db_project")
        cur.close()
        conn.close()

    def connect(self):
        """Соединение с базой данных"""
        return psycopg2.connect(
            dbname=self.dbname, user=self.user, password=self.password, host=self.host, port=self.port
        )

    def create_tables(self):
        """Создание таблиц компаний и вакансий"""
        with self.conn, self.conn.cursor() as cur:
            cur.execute(
                """
            CREATE TABLE IF NOT EXISTS companies (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                url VARCHAR(255),
                hh_id VARCHAR(255) UNIQUE
            );
            """
            )

            cur.execute(
                """
            CREATE TABLE IF NOT EXISTS vacancies (
                id SERIAL PRIMARY KEY,
                title VARCHAR(255),
                salary_from INT,
                salary_to INT,
                url VARCHAR(255) UNIQUE,
                area VARCHAR(255),
                company_id VARCHAR(255),
                FOREIGN KEY (company_id) REFERENCES companies(hh_id)
            );
            """
            )

    def add_company(self, name: str, url: str, hh_id: str):
        """Метод добавления компании"""
        with self.conn, self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO companies (name, url, hh_id)
                VALUES (%s, %s, %s)
                ON CONFLICT (hh_id) DO NOTHING
                RETURNING id;
            """,
                (name, url, hh_id),
            )

    def add_vacancy(self, title: str, salary_from: int, salary_to: int, url: str, area: str, company_id: int):
        """Метод добавления вакансии"""
        with self.conn, self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO vacancies (title, salary_from, salary_to, url, area, company_id)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (url) DO NOTHING;
            """,
                (title, salary_from, salary_to, url, area, company_id),
            )

    def get_companies_and_vacancies_count(self):
        """Метод для получения списков всех компаний и количество вакансий у каждой компании."""
        query = """
                SELECT c.name, COUNT(v.id) AS vacancies_count
                FROM companies c
                LEFT JOIN vacancies v ON c.hh_id = v.company_id
                GROUP BY c.name
                ORDER BY vacancies_count DESC;
            """
        with self.conn.cursor() as cur:
            cur.execute(query)
            result = cur.fetchall()
            return [{"company": row[0], "vacancies_count": row[1]} for row in result]

    def get_all_vacancies(self):
        """Метод для получения списка всех вакансий с указанием названия компании, названия вакансии, зарплаты, ссылки на вакансию."""
        query = """
            SELECT 
                c.name AS company_name,
                v.title AS vacancy_title,
                v.salary_from,
                v.salary_to,
                v.url AS vacancy_url
            FROM vacancies v
            JOIN companies c ON v.company_id = c.hh_id
            ORDER BY c.name;
        """
        with self.conn.cursor() as cur:
            cur.execute(query)
            result = cur.fetchall()
            return [
                {
                    "company": row[0],
                    "vacancy": row[1],
                    "salary_from": row[2],
                    "salary_to": row[3],
                    "url": row[4],
                }
                for row in result
            ]

    def get_avg_salary(self):
        """Метод для получения средней зарплаты по вакансиям"""
        query = """
            SELECT AVG((COALESCE(salary_from, 0) + COALESCE(salary_to)) / 2.0) AS avg_salary
            FROM vacancies
            WHERE salary_from IS NOT NULL OR salary_to IS NOT NULL;
        """
        with self.conn.cursor() as cur:
            cur.execute(query)
            result = cur.fetchone()
            return result[0] if result else None

    def get_vacancies_with_higher_salary(self):
        """Метод для получения списка всех вакансий, у которых зарплата выше средней по всем вакансиям."""
        avg_salary_query = """
            SELECT AVG((COALESCE(salary_from, 0) + COALESCE(salary_to, 0)) / 2.0) AS avg_salary
            FROM vacancies
            WHERE salary_from IS NOT NULL OR salary_to IS NOT NULL;
        """

        with self.conn.cursor() as cur:
            # Получаем среднюю зарплату
            cur.execute(avg_salary_query)
            avg_salary_result = cur.fetchone()
            avg_salary = avg_salary_result[0] if avg_salary_result else 0

            # Получаем вакансии с зарплатой выше средней
            query = """
                SELECT 
                    c.name AS company_name,
                    v.title AS vacancy_title,
                    v.salary_from,
                    v.salary_to,
                    v.url AS vacancy_url
                FROM vacancies v
                JOIN companies c ON v.company_id = c.hh_id
                WHERE (COALESCE(v.salary_from, 0) + COALESCE(v.salary_to, 0)) / 2.0 > %s
                ORDER BY v.salary_from DESC NULLS LAST;
            """
            cur.execute(query, (avg_salary,))
            rows = cur.fetchall()

            return [
                {
                    "company": row[0],
                    "vacancy": row[1],
                    "salary_from": row[2],
                    "salary_to": row[3],
                    "url": row[4],
                }
                for row in rows
            ]

    def get_vacancies_with_keyword(self, keyword: str):
        """Метод для получения списка всех вакансий, в названии которых содержатся переданные слова"""
        if not keyword:
            return []

        query = """
            SELECT 
                c.name AS company_name,
                v.title AS vacancy_title,
                v.salary_from,
                v.salary_to,
                v.url AS vacancy_url
            FROM vacancies v
            JOIN companies c ON v.company_id = c.hh_id
            WHERE v.title ILIKE %s
            ORDER BY v.salary_from DESC NULLS LAST;
        """

        with self.conn.cursor() as cur:
            cur.execute(query, (f"%{keyword}%",))
            rows = cur.fetchall()

            return [
                {
                    "company": row[0],
                    "vacancy": row[1],
                    "salary_from": row[2],
                    "salary_to": row[3],
                    "url": row[4],
                }
                for row in rows
            ]

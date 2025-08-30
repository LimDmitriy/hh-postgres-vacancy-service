from src.db_manager import DBManager
from src.hh_api import HeadHunterAPI

api = HeadHunterAPI()
db = DBManager()
db.create_tables()

while True:
    print("1. Добавление новых компаний и вакансий.")
    print("2. Список всех компаний и кол-во их вакансий.")
    print("3. Список всех вакансий.")
    print("4. Средняя зп всех вакансий.")
    print("5. Список вакансий с зп выше средней.")
    print("6. Поиск вакансий по ключевым словам")
    print("7. Выход")

    user_input = input("Выберите пункт меню:\n")

    if user_input == "1":
        query = input("Введите название компании: ")
        companies = api.get_companies(query)
        for company in companies:
            company_name = company.get("name") or "Без названия"
            company_url = company.get("url") or ""
            company_hh_id = company.get("hh_id") or ""
            db.add_company(name=company_name, url=company_url, hh_id=company_hh_id)

            vacancies = api.get_company_vacancies(company_hh_id)

            for vac in vacancies:
                title = vac.get("name") or "Без названия"
                salary_from = vac.get("salary_from")
                salary_to = vac.get("salary_to")
                salary_from = salary_from if isinstance(salary_from, int) else None
                salary_to = salary_to if isinstance(salary_to, int) else None
                area = vac.get("area") or "Не указано"
                url = vac.get("alternate_url") or ""
                db.add_vacancy(
                    title=title,
                    salary_from=salary_from,
                    salary_to=salary_to,
                    area=area,
                    url=url,
                    company_id=company_hh_id,
                )
        print("Компании добавлены!")

    elif user_input == "2":
        data = db.get_companies_and_vacancies_count()
        for i in data:
            print(f'Компания: {i["company"]}. Количество вакансий: {i["vacancies_count"]}')

    elif user_input == "3":
        data = db.get_all_vacancies()
        for i in data:
            print(f"{i['company']} | {i['vacancy']} | {i['salary_from']} - {i['salary_to']} | {i['url']}")

    elif user_input == "4":
        avg_salary = db.get_avg_salary()
        if avg_salary:
            print(f"Средняя зарплата по всем вакансиям: {avg_salary:.2f}")
        else:
            print("Нет данных о зарплатах.")

    elif user_input == "5":
        vacancies = db.get_vacancies_with_higher_salary()
        for v in vacancies:
            print(f"{v['vacancy']} | {v['salary_from']} - {v['salary_to']} | {v['url']}")

    elif user_input == "6":
        keyword = input("Введите ключевое слово для поиска: ")
        vacancies = db.get_vacancies_with_keyword(keyword)
        for v in vacancies:
            print(f"{v['vacancy']} | {v['salary_from']} - {v['salary_to']} | {v['url']}")

    elif user_input == "7":
        print("Выход из программы.")
        break

    else:
        print("Некорректный ввод, попробуйте снова.")

from abc import ABC, abstractmethod
from typing import Any, Dict, List

import requests


class BasicClass(ABC):
    """Абстрактный класс для работы с API"""

    @abstractmethod
    def _connect_api(self, url: "str", params: dict[str, Any]) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def get_companies(self, text: str, per_page: int = 10) -> list[Dict[str, Any]]:
        pass

    @abstractmethod
    def get_company_vacancies(self, company_id: str, per_page: int = 20) -> List[Dict[str, Any]]:
        pass


class HeadHunterAPI(BasicClass):
    """Класс для работы с API сервиса с компаниями и вакансиями"""

    def __init__(self) -> None:
        self._company_url = "https://api.hh.ru/employers"
        self._vacancies_url = "https://api.hh.ru/vacancies"

    def _connect_api(self, url: "str", params: dict[str, Any]) -> List[Dict[str, Any]]:
        """Метод для подключения к API и получения данных"""
        try:
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                return data.get("items", [])
            else:
                print(f"Ошибка запроса: {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"Ошибка запроса: {e}")
            return []

    def get_companies(self, text: str, per_page: int = 10) -> list[Dict[str, Any]]:
        """Метод для получение списка компаний по ключевому слову"""
        if not text:
            raise ValueError("Введите название компании или сферу")
        params = {"text": text, "per_page": per_page}
        companies = self._connect_api(self._company_url, params)
        result = []
        for company in companies:
            result.append(
                {
                    "name": company.get("name"),
                    "url": company.get("alternate_url"),
                    "hh_id": str(company.get("id")),
                }
            )
        return result

    def get_company_vacancies(self, company_id: str, per_page: int = 20) -> List[Dict[str, Any]]:
        """Метод для получение вакансий компании по её ID"""
        params = {"employer_id": company_id, "per_page": per_page}
        vacancies = self._connect_api(self._vacancies_url, params)
        result = []
        for vac in vacancies:
            salary_data = vac.get("salary")

            if isinstance(salary_data, dict):
                salary_from = salary_data.get("from") or 0
                salary_to = salary_data.get("to") or 0
            elif isinstance(salary_data, int):
                salary_from = salary_to = salary_data
            else:
                salary_from = salary_to = 0
            title = vac.get("name") or "Без названия"
            area = vac.get("area", {}).get("name") or "Не указано"
            url = vac.get("alternate_url") or ""
            result.append(
                {
                    "name": title,
                    "salary": salary_from or salary_to,  # используем from, если нет, то to
                    "salary_from": salary_from if salary_from else "Не указано",
                    "salary_to": salary_to if salary_to else "Не указано",
                    "area": area,
                    "alternate_url": url,
                    "company_id": company_id,
                }
            )

        return result

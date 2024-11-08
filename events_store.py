class EventStore:

    def __init__(self, max_events_per_user=10):

        self.events = {}
        self.max_events_per_user = max_events_per_user

    def put(self, user_id, item_id):
        """
        Сохраняет событие
        """
        try:
            user_events = self.events[user_id]
            self.events[user_id] = [item_id] + user_events[: self.max_events_per_user]
        except KeyError:
            self.events[user_id] = [item_id]

    def get(self, user_id, k):
        """
        Возвращает события для пользователя
        """
        try: 
            user_events = self.events[user_id][:k]
        except KeyError:
           user_events = []

        return user_events

def form_items_to_directors(form_items):
    if 'Item 5.02' in form_items:
        return form_items['Item 5.02']
    else:
        return None

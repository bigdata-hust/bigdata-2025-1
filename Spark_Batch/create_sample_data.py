#!/usr/bin/env python3
"""
Create Sample Yelp Data for Testing
Generates small synthetic datasets for testing the pipeline
"""
import json
import random
from datetime import datetime, timedelta
import os


def generate_business_data(num_businesses=100):
    """Generate sample business data"""
    cities = ['Phoenix', 'Las Vegas', 'Toronto', 'Cleveland', 'Charlotte']
    states = ['AZ', 'NV', 'ON', 'OH', 'NC']
    categories_options = [
        'Restaurants, Food',
        'Shopping, Retail',
        'Food, Coffee & Tea',
        'Beauty & Spas, Hair Salons',
        'Home Services, Plumbing',
        'Automotive, Auto Repair',
        'Health & Medical, Doctors',
        'Nightlife, Bars',
        'Active Life, Gyms',
        'Hotels & Travel, Hotels'
    ]

    businesses = []
    for i in range(num_businesses):
        business = {
            'business_id': f'business_{i:05d}',
            'name': f'Business {i}',
            'address': f'{random.randint(100, 9999)} Main St',
            'city': random.choice(cities),
            'state': random.choice(states),
            'postal_code': f'{random.randint(10000, 99999)}',
            'latitude': round(random.uniform(33.0, 45.0), 6),
            'longitude': round(random.uniform(-120.0, -80.0), 6),
            'stars': round(random.uniform(1.0, 5.0), 1),
            'review_count': random.randint(5, 500),
            'is_open': random.choice([0, 1]),
            'categories': random.choice(categories_options),
            'attributes': None,
            'hours': None
        }
        businesses.append(business)

    return businesses


def generate_review_data(num_reviews=1000, num_businesses=100):
    """Generate sample review data"""
    reviews = []
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2023, 12, 31)

    for i in range(num_reviews):
        business_id = f'business_{random.randint(0, num_businesses-1):05d}'
        review_date = start_date + timedelta(
            seconds=random.randint(0, int((end_date - start_date).total_seconds()))
        )

        review = {
            'review_id': f'review_{i:06d}',
            'business_id': business_id,
            'user_id': f'user_{random.randint(0, 500):05d}',
            'stars': float(random.randint(1, 5)),
            'useful': random.randint(0, 20),
            'funny': random.randint(0, 10),
            'cool': random.randint(0, 10),
            'text': f'Sample review text {i}. Great experience!',
            'date': review_date.strftime('%Y-%m-%d %H:%M:%S')
        }
        reviews.append(review)

    return reviews


def save_json_lines(data, filename):
    """Save data as JSON lines format"""
    with open(filename, 'w', encoding='utf-8') as f:
        for item in data:
            f.write(json.dumps(item) + '\n')
    print(f'✓ Created {filename} with {len(data)} records')


def main():
    """Generate sample data"""
    print('='*60)
    print('Creating Sample Yelp Data for Testing')
    print('='*60)

    # Create data directory if not exists
    data_dir = '../data'
    os.makedirs(data_dir, exist_ok=True)

    # Generate data
    print('\nGenerating business data...')
    businesses = generate_business_data(num_businesses=100)
    save_json_lines(businesses, os.path.join(data_dir, 'business.json'))

    print('\nGenerating review data...')
    reviews = generate_review_data(num_reviews=1000, num_businesses=100)
    save_json_lines(reviews, os.path.join(data_dir, 'review.json'))

    print('\n' + '='*60)
    print('✓ Sample data created successfully!')
    print('='*60)
    print(f'\nData location: {data_dir}/')
    print('  - business.json (100 businesses)')
    print('  - review.json (1000 reviews)')
    print('\nYou can now run the pipeline:')
    print('  ./run_local.sh')
    print('='*60)


if __name__ == '__main__':
    main()

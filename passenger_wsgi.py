import sys, os

# Setup for cPanel Python App / Phusion Passenger
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from app import app as application

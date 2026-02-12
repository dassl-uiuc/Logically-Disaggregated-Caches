sudo apt-get update
sudo apt-get upgrade -y

echo "Installing python3-pip..."
sudo apt install python3-pip -y

echo "Installing python dependencies..."
pip install -r requirements.txt

echo "Python dependencies installed successfully."
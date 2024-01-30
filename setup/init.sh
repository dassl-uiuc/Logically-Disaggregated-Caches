sudo apt-get update
sudo apt-get upgrade -y

echo "Installing python3-pip..."
sudo apt install python3-pip -y

echo "Installing python dependencies..."
pip install -r requirements.txt

echo "Python dependencies installed successfully."

echo "Running init_setup.sh..."
./init_setup.sh

echo "init_setup.sh completed successfully."

echo " ***************************************************"
echo " IMPORTANT: Reboot all the hosts in cloudlab before running the experiment."
echo " ***************************************************"
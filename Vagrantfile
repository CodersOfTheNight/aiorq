Vagrant.configure(2) do |config|
  config.vm.box = "hashicorp/precise32"

  config.vm.synced_folder ".", "/vagrant", disabled: true
  config.vm.synced_folder ".", "/home/vagrant/aiorq"

  config.vm.provision "shell" do |script|
    script.path = "scripts/deploy.sh"
    script.keep_color = true
  end
end

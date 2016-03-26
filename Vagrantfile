Vagrant.configure(2) do |config|
  config.vm.box = "hashicorp/precise32"

  config.vm.synced_folder ".", "/vagrant", disabled: true
  config.vm.synced_folder ".", "/home/vagrant/aiorq"

  config.vm.provision "ansible" do |ansible|
    ansible.playbook = "playbook.yml"
  end

  config.vm.network "forwarded_port", guest: 9181, host: 9181
end

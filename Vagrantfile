Vagrant.configure(2) do |config|
  config.vm.box = "hashicorp/precise32"

  config.vm.provision "shell" do |script|
    script.path = "scripts/deploy.sh"
    script.keep_color = true
  end
end
